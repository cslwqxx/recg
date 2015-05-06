#This class is used to calculate a guessed amount of memory the given object requires.
class ObjectSizeCounter
  def initialize(obj)
    @checked = {}
    @object = obj
  end

  def calculate_size
    ret = self.var_size(@object)
    @checked = nil
    @object = nil
    return ret
  end

  def object_size(obj)
    size = 0

    obj.instance_variables.each do |var_name|
      var = obj.instance_variable_get(var_name)
      next if @checked.key?(var.__id__)
      @checked[var.__id__] = true
      size += self.var_size(var)
    end

    return size
  end

  def var_size(var)
    size = 0

    if var.is_a?(String)
      size += var.length
    elsif var.is_a?(Integer)
      size += var.to_s.length
    elsif var.is_a?(Symbol) or var.is_a?(Fixnum)
      size += 4
    elsif var.is_a?(Time)
      size += var.to_f.to_s.length
    elsif var.is_a?(Hash)
      var.each do |key, val|
        size += self.var_size(key)
        size += self.var_size(val)
      end
    elsif var.is_a?(Array)
      var.each do |val|
        size += self.object_size(val)
      end
    elsif var.is_a?(TrueClass) or var.is_a?(FalseClass)
      size += 1
    else
      size += self.object_size(var)
    end

    return size
  end
end