# Include this into a class
# Example:
#   class IndexEngine
#     include DataflowEngine
#   end
module Dataflow
  module Engine

    def self.included(base)
      base.extend(Dataflow::Engine::ClassMethods)
    end

    module ClassMethods

      # Declares the start of a dataflow.
      # While you can pass a namespace for the dataflow, it currently doesn't do anything.
      def dataflow(namespace, &block)
        @@serialize_as = nil
        yield
      end

      # Serialize messages. Defaults to json.
      def serialize_as(serialize_method)
        @@serialize_as = serialize_method
      end

      # Sometimes you want to pass dataset references instead of the dataset itself
      # This allows you to transform the dataset reference into a dataset before
      # passing it through the dataflow processor.
      def fetch_dataset(fetch_dataset_method)
        @@fetch_dataset = fetch_dataset_method
      end

      # FIXME: HACK
      # This defines the name of the association used for journalling the state
      # This should be standardized through metaprogramming so that the 
      # developer doesn't even think about it
      def journal_with(journal_with)
        @@journal_with = journal_with
      end

      # Declares a starting point. Datasets will be injected through this stage.
      def start_with(name, &run_block)
        @@starter = processors(name)
        @@starter.run_block = run_block
      end

      # Declares an end point. This will mark a dataset as being completed
      def ends_with(name)
        @@endpoint = processors(name)
        @@endpoint.end_point = true
      end

      # Declares a transformation stage
      def change(inputs, output, &run_block)
        output = processors(output)
        unless inputs.kind_of?(Array)
          inputs = [ inputs ]
        end
        output.run_block = run_block if run_block # FIXME: Hack. What if we want multiple blocks?

        inputs.each do |input|
          input = processors(input)
          output.add_input(input)
          input.add_output(output)
        end
      end

      # Adds rescue handling for a stage
      def rescue_change(name, &rescue_block)
        processors(name).rescue_block = rescue_block
      end

      # Injects dataset messages into the Engine
      def inject(*dataset_refs)
        EM.run do
          dataset_refs.flatten.each do |refs|
            @@starter << refs
          end
        end
      end

      # Spin up all declared dataflow processors
      # You would call this from Merb.after_app_load
      def start
        processors.each do |q, processor| 
          processor.start
        end
      end

      # Processes a dataset with a named processor. Logging is done, but queuing is not.
      # This is useful for testing
      def process_only(stage, dataset)
        processors(stage).process(dataset)
      end

      # Returns a Hash with all the named processors. 
      # Optionally, return the processor for a given name
      def processors(name = nil)
        @@processors ||= Hash.new
        return @@processors unless name
        processors[name] ||= Dataflow::Processor.new(name.to_s, 
          :serialize_as => @@serialize_as, 
          :fetch_dataset => @@fetch_dataset,
          :journal => @@journal_with
          )
      end

    end
  end
end
