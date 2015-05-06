require 'monitor'
require 'camera_helper'
require 'detector'
require 'ruboto/util/stack'

#import org.opencv
class Detector
  extend MonitorMixin

  @@detector = nil
  @@camera_data = nil

  def self.start(activity, camera)
    Thread.with_large_stack(512) do
      synchronize do
        if @@detector.nil?
          activity.server_status = "Detecting"
		  
          @@detector = Thread.new{
			java_import 'org.opencv.core.Mat'
			count = 0
		  	loop do
				CameraHelper.take_picture(self, camera, activity)
				
				
				@@camera_data = nil
				activity.server_status = "taking picture No.#{count}..."
				sleep 1
				count += 1
			end		  
		  }
        end
        activity.server_status = "WEBrick started on"
      end
    end
  end

  def self.camera_data=(data)
    @@camera_data = data
  end

  def self.stop
    synchronize do
      if @@detector
        @@detector.shutdown
        sleep 0.1 until @@detector.status == :Stop
        @@detector = nil
      end
    end
  end

end