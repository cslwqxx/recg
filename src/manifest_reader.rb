require_relative 'service_node'
module ManifestReader
  def self.read_app_manifest(app_name)
		Dir.mkdir(APP_MANIFESTS_ROOT_DIR) unless File.directory?(APP_MANIFESTS_ROOT_DIR)
    manifest_dir = File.expand_path("#{app_name}.xml", APP_MANIFESTS_ROOT_DIR)

    #unless File.exist? manifest_dir
      #todo:check the manifest version
      get_app_manifest app_name, manifest_dir
    #end
    info = {}
#p info
    file = File.new(manifest_dir)
    doc = Document.new(file)
    root = doc.root
    info[:author] = root.attributes['author']
#p info
    param = {}
    root.elements['parameters'].each_element('parameter') do |e|
      name = e.attributes['name']
      value = e.attributes['default-value']
      type = e.attributes['type']
      max = e.attributes['max']
      min = e.attributes['min']

      val = value
      case type
        when 'Integer'
          val = value.to_i
          max = max.to_i
          min = min.to_i
        when 'Float'
          val = value.to_f
          max = max.to_f
          min = min.to_f
        else
      end
      param[name.to_sym] = {:default_value => val,
                            :type => type,
                            :max => max,
                            :min => min}
    end
#p param
    app = root.elements['application']
    info[:app_name] = app.attributes['name']
    module_hash = {}
    app.each_element('module') do |e|
      name = e.attributes['name'].to_sym
      module_hash[name] = {}
      module_hash[name][:related_module] = {}
    end
    client_module_name = nil
    app.each_element('module') do |e|
      module_name = e.attributes['name'].to_sym
      module_entrance_file = e.attributes['entrance-file']
      module_location = e.attributes['location']
      module_reusable = e.attributes['reusable']
      module_hash[module_name][:name] = module_name
      module_hash[module_name][:entrance_file] = module_entrance_file
      module_hash[module_name][:location] = module_location
      module_hash[module_name][:reusable] = module_reusable
      if client_module_name === nil and module_location === 'client'
        client_module_name = module_name
      end
      resources_dir_arr = []
      e.each_element('resource') do |r|
        resources_dir_arr.push([r.attributes['const-name'],r.attributes['dir']])
      end
      module_hash[module_name][:resources] = resources_dir_arr
      require_files_arr = []
      e.each_element('require-file') do |rf|
        require_files_arr.push(rf.attributes['name'])
      end
=begin
      require_jars_arr = []
      if ServiceNode.instance.type == :server
        e.each_element('server-require-jar') do |rf|
          require_jars_arr.push(rf.attributes['name'])
        end
      else
        e.each_element('mobile-require-jar') do |rf|
          require_jars_arr.push(rf.attributes['name'])
        end
      end
=end
      adjustable_parameter_arr = []
      e.each_element('adjustable-parameter') do |ap|
        adjustable_parameter_arr.push(ap.attributes['name'].to_sym)
      end
      module_hash[module_name][:adjustable_parameters] = adjustable_parameter_arr
      module_hash[module_name][:require_files] = require_files_arr
#      module_hash[module_name][:require_jars] = require_jars_arr
      e.each_element('related-module') do |rm|
        rm_name = rm.attributes['name'].to_sym
        rm_grouping_type = rm.attributes['grouping']
        module_hash[module_name][:related_module][rm_name] = [module_hash[rm_name], rm_grouping_type]
      end
    end
#p module_hash
    module_hash.each_pair do |name, m|
      m[:related_module].each_key do |rm_name|
        module_hash[rm_name][:precursor_module] ||= []
        module_hash[rm_name][:precursor_module].push name
      end
    end
    info[:app_module_hash] = module_hash
    info[:client_module_name] = client_module_name.to_sym
    info[:param] = param
    info
  end

  private

  def self.get_app_manifest (app_name, manifest_dir)
    Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
      request = Net::HTTP::Get.new "/get_app_manifest?name=#{app_name}"
      http.request request do |response|
        open manifest_dir, 'w' do |io|
          response.read_body do |chunk|
            io.write chunk
          end
        end
      end
    end
  end
end