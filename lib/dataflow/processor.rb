
module Dataflow
  class Processor
    attr_accessor :name, :run_block, :undo_block, :rescue_block, :serialize_as, :fetch_dataset, :journal
    attr_accessor :end_point

    def initialize(name, opt = {})
      @name = name
      @run_block = opt[:run_block]
      @undo_block = opt[:undo_block]
      @rescue_block = opt[:rescue_block]
      @serialize_as = opt[:serialize_as] || :json
      @fetch_dataset = opt[:fetch_dataset] || nil
      @end_point = opt[:end_point] || false
      @journal = opt[:journal] || :journal_entries
    end

    def push(obj)
      publisher.publish(serialize_object(obj))
    end

    alias << push

    def start
      raise "run_block required for #{self.inspect}" unless @run_block
      Thread.new do
        EM.run do
          mq = MQ.queue(@name).subscribe(:ack => true) do |info, msg|
            begin
              obj = deserialize_object(msg)
              dataset = @fetch_dataset.call(obj) if @fetch_dataset
              dataset ||= obj
              if inputs.size > 1
                next unless completed_all_inputs(dataset)
              end
              if process(dataset)
                info.ack
                # FIXME: This does not work.
                if @end_point
                  obj.update_completed
                else
                  outputs.each do |output|
                    output.push(obj)
                  end
                end
              end
            rescue => e
              @rescue_block.call(e, dataset) if @rescue_block
              info.ack # Punt off to error handling
            end
          end
        end
      end
    end

    def add_input(input)
      inputs.push(input).uniq!
      @input_queue_names = nil
      inputs
    end

    def add_output(output)
      outputs.push(output).uniq!
    end

    def inputs
      @inputs ||= []
    end

    def outputs
      @outputs ||= []
    end

    def process(obj)
      obj.log_start(@name)
      return obj.log_complete(@name) if @run_block.call(obj)
    end

    # Debug

    def inspect
      out_inspect = outputs.map { |o| o.to_s }.join(',')
      in_inspect = inputs.map { |i| i.to_s }.join(',')
      "#<#{self.class}:#{object_id} #{@name}, Inputs: [#{in_inspect}], Outputs: [#{out_inspect}]>"
    end

    def to_s
      "#<#{self.class}:#{object_id} #{@name}>"
    end

    private

    def publisher
      @publisher ||= MQ.queue(self.name)
    end

    def serialize_object(obj)
      case @serialize_as
      when :json
        JSON.generate(obj)
      when :ruby
        Marshall.dump(obj)
      else
        obj # nil should be bare object
      end
    end

    def deserialize_object(obj)
      case @serialize_as
      when :json
        JSON.parse(obj)
      when :ruby
        Marshall.load(obj)
      else
        obj
      end
    end

    def input_queue_names
      @input_queue_names ||= inputs.map { |i| i.name }.compact
    end

    def completed_all_inputs(dataset)
      (input_queue_names - dataset.send(@journal).completed(*input_queue_names)).empty?
    end
  end
end
