class Optimizer
  def initialize (app_name)
    set_learned_dependency app_name
  end

  def set_learned_dependency (app_name)
=begin
    Dir.mkdir(APP_MANIFESTS_ROOT_DIR) unless File.directory?(APP_MANIFESTS_ROOT_DIR)
    dependency_dir = File.expand_path("#{app_name}_learned_dependency.rb", APP_MANIFESTS_ROOT_DIR)
    Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
      request = Net::HTTP::Get.new "/get_app_learned_dependency?name=#{app_name}"
      http.request request do |response|
        open dependency_dir, 'w' do |io|
          response.read_body do |chunk|
            io.write chunk
          end
        end
      end
    end
    instance_eval(File.read(dependency_dir))
=end
  end

end