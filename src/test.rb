require 'json'
TARGET_MODULE = 'data_size'
if TARGET_MODULE == 'data_size'
  data_hash = {}
  open('exec_report_4_network.data', 'r') { |f|
    until f.eof?
      line = f.readline
      record = JSON.parse(line)
      token = record['events'][0]['token']
      if record['component'] == 'Preprocessor_BuildingDetection_lwq'
        unless data_hash.has_key? token
          data_hash[token] = {:compress_ratio => record['params']['compress_ratio'], :data_size =>{}}
        end
      end
      record['events'].each do |e|
        if e['type'] == 'sent' and e['data_size'] != 'unknown'
          data_hash[token] = {:compress_ratio => :unknown, :data_size =>{}} unless data_hash.has_key? token
          data_hash[token][:data_size][record['component'].split('_')[0].downcase.to_sym] = e['data_size'].to_i
        end
      end
    end
  }
  ratio_hash = {}
  open('Client_sent_size_plot.dat', 'w') { |client|
  open('Preprocessor_sent_size_plot.dat', 'w') { |preprocessor|
  open('FeatureExtractor_sent_size_plot.dat', 'w') { |featureextractor|
  open('FingerprintMaker_sent_size_plot.dat', 'w') { |fingerprintmaker|
  open('ImageMatcher_sent_size_plot.dat', 'w') { |imagematcher|
    data_hash.each_value { |h|
      h[:data_size].each_pair { |name, size|
        unless h[:compress_ratio] == :unknown
          eval "#{name.to_s}.puts \"#{h[:compress_ratio]} #{size}\""
          ratio = h[:compress_ratio].to_f
          ratio_hash[ratio] = {} unless ratio_hash.has_key? ratio
          ratio_hash[ratio][name] = {:count => 0, :sum => 0} unless ratio_hash[ratio].has_key? name
          ratio_hash[ratio][name][:count] += 1
          ratio_hash[ratio][name][:sum] += size
        end
      }
    }
  }}}}}
  open('Client_sent_size_avg.dat', 'w') { |client|
  open('Preprocessor_sent_size_avg.dat', 'w') { |preprocessor|
  open('FeatureExtractor_sent_size_avg.dat', 'w') { |featureextractor|
  open('FingerprintMaker_sent_size_avg.dat', 'w') { |fingerprintmaker|
  open('ImageMatcher_sent_size_avg.dat', 'w') { |imagematcher|
    ratio_hash.each_pair { |ratio, name_h|
      name_h.each_pair { |name, v|
        avg = 1.0 * v[:sum] / v[:count]
        eval "#{name.to_s}.puts \"#{ratio} #{format('%.5f',avg)}\""
      }
    }
  }}}}}
  module_arr = %w(Client Preprocessor FeatureExtractor FingerprintMaker ImageMatcher)
  module_arr.each do |name|
    min_ratio = Float::MAX
    max_ratio = 0
    min_size = Float::MAX
    max_size = 0
    open("#{name}_sent_size_avg.dat", 'r') do |f|
      until f.eof?
        line = f.readline
        ratio, size = line.split(' ')
        ratio = ratio.to_f
        size = size.to_f
        max_ratio = ratio if ratio > max_ratio
        min_ratio = ratio if ratio < min_ratio
        max_size = size if max_size < size
        min_size = size if min_size > size
      end
    end
    puts "#{name}\tmin ratio:#{min_ratio} ratio range:#{format('%.5f', max_ratio - min_ratio)} min size:#{min_size} size range:#{format('%.5f', max_size - min_size)} "
    open("#{name}_sent_size_avg.dat", 'r') do |f|
      open("#{name}_sent_size.dat", 'w') do |out|
        until f.eof?
          line = f.readline
          ratio, size = line.split(' ')
          ratio = ratio.to_f
          size = size.to_f
          normalized_ratio = (ratio - min_ratio) / (max_ratio - min_ratio)
          normalized_size = (size - min_size) / (max_size - min_size)
          out.puts "#{normalized_ratio},#{normalized_size}"
        end
      end
    end
  end

else
  cloud_num = 0
  mobile_num = 0
  cloud_exec = 0
  mobile_exec = 0
  compress_ratio_hash = {}
  min_compress = Float::MAX
  max_compress = 0
  min_time = Float::MAX
  max_time = 0
  open("#{TARGET_MODULE}_tmp.dat", 'w') { |out|
    open('exec_report_transfer_data_volume.data', 'r') { |f|
      until f.eof?
        line = f.readline
        record = JSON.parse(line)
        if record['component'] == 'Preprocessor_BuildingDetection_lwq'
          min_compress = record['params']['compress_ratio'] if record['params']['compress_ratio'] < min_compress
          max_compress = record['params']['compress_ratio'] if record['params']['compress_ratio'] > max_compress
          if compress_ratio_hash.has_key? record['events'][0]['token']
            out.puts "#{record['params']['compress_ratio']},#{compress_ratio_hash[record['events'][0]['token']]}"
          else
            compress_ratio_hash[record['events'][0]['token']] = record['params']['compress_ratio']
          end
        end
        if record['component'] == "#{TARGET_MODULE}_BuildingDetection_lwq"
          if record['location'] == 'cloud'
            cloud_num += 1
          else
            mobile_num += 1
          end
          start_time = 0
          finish_time = 0
          blocked_time = 0
          record['events'].each do |e|
            start_time = e['timestamp'] if e['type'] == 'started'
            if e['type'] == 'finished'
              finish_time = e['timestamp']
              blocked_time = e['blocked_time']
            end
          end
          exec_time = finish_time - start_time - blocked_time
          exec_time = format('%.5f',exec_time).to_f
          next if exec_time < 0

          min_time = exec_time if exec_time < min_time
          max_time = exec_time if exec_time > max_time
          if record['location'] == 'cloud'
            cloud_exec += exec_time
          else
            mobile_exec += exec_time
          end
          #puts "exec_time:#{exec_time}\t#{record['location']}"
          #out.puts "#{record['params'].to_json},#{record['location']},#{exec_time}"
          if compress_ratio_hash.has_key? record['events'][0]['token']
            out.puts "#{compress_ratio_hash[record['events'][0]['token']]},#{record['location']=='cloud'?1:0},#{exec_time}"
          else
            compress_ratio_hash[record['events'][0]['token']] = "#{record['location']=='cloud'?1:0},#{exec_time}"
          end
        end
      end
    }
  }
  open("#{TARGET_MODULE}_cloud_plot.dat", 'w') { |out|
    open("#{TARGET_MODULE}_tmp.dat", 'r') { |f|
      until f.eof?
        compress_r, loc, time = f.readline.split(',')
        out.puts "#{compress_r} #{time}" if loc == '1'
      end
    }
  }
  open("#{TARGET_MODULE}_mobile_plot.dat", 'w') { |out|
    open("#{TARGET_MODULE}_tmp.dat", 'r') { |f|
      until f.eof?
        compress_r, loc, time = f.readline.split(',')
        out.puts "#{compress_r} #{time}" if loc == '0'
      end
    }
  }
  params_hash = {}
  open("#{TARGET_MODULE}_tmp.dat", 'r') { |f|
    until f.eof?
      compress_r, loc, time = f.readline.split(',')
      hash_code = "#{compress_r},#{loc}"
      params_hash[hash_code] = {:count => 0, :sum => 0} unless params_hash.has_key? hash_code
      params_hash[hash_code][:count] += 1
      params_hash[hash_code][:sum] += time.to_f

    end
  }
  open("#{TARGET_MODULE}_avg.dat", 'w') { |out|
    params_hash.each_pair do |k, v|
      out.puts "#{k},#{format('%.5f',v[:sum]/v[:count])}"
    end
  }
  time_range = max_time - min_time
  compress_range = max_compress - min_compress
  open("#{TARGET_MODULE}.dat", 'w') { |out|
    #out.puts "#{min_compress},#{compress_range}:#{min_time},#{time_range}"
    open("#{TARGET_MODULE}_avg.dat", 'r') { |f|
      until f.eof?
        compress_r, loc, time = f.readline.split(',')
        out.puts "#{(compress_r.to_f - min_compress)/compress_range},#{loc},#{(time.to_f - min_time)/time_range}"
      end
    }
  }
  puts "time_range:[#{min_time},#{max_time}](#{time_range})\tcompress_ratio_range:[#{min_compress},#{max_compress}](#{compress_range})"
  puts "cloud_num:#{cloud_num}\tmobile_num:#{mobile_num}"
  puts "cloud_avg_exec:#{cloud_exec/cloud_num}\tmobile_avg_exec:#{mobile_exec/mobile_num}"

end

=begin
require 'object_size_counter'
require 'base64'
a = {
    'a' => 1,
    'b' => ['2',5,:teso]
}
p ObjectSizeCounter.new(a).calculate_size
p Marshal.dump(a).size
s = Time.now
100000.times do
  ObjectSizeCounter.new(a).calculate_size
end
p e = Time.now - s
s = Time.now
100000.times do
  Marshal.dump(a).size
end
p e = Time.now - s
=end
=begin
tmp = nil
File.open('probe_record.dat', 'rb') { |file|
  tmp = file.read
}
ratio = 0.1
probe_record = Marshal.load tmp
p probe_record[ratio]
probe_record[ratio].each do |name, v|
  puts "#{name}\t#{v[:server][:sum]*v[:client][:count]/(v[:client][:sum]*v[:server][:count])}"
end
puts '-------------------------------------------------'
probe_record[ratio].each do |name, v|
  puts "#{name}\tClient:#{1.0*v[:client][:sum]/v[:client][:count]}\tServer:#{1.0*v[:server][:sum]/v[:server][:count]}"
end
=end