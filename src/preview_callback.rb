#export LC_CTYPE=en_US.UTF-8
#import android.graphics.BitmapFactory
import android.graphics.YuvImage
import android.graphics.ImageFormat
import android.graphics.Rect

java_import 'java.io.ByteArrayOutputStream'

require 'base64'

class PreviewCallback
  def initialize(activity)
    @activity = activity
  end

  def onPreviewFrame(data, camera) 
    unless @activity.client_instance === nil
      #@activity.server_status = "processing frame No.#{@count}..."

      tmp2 = [data, camera]
      #tmp = jdata.to_a.pack('C*').force_encoding('utf-8')    #this way can accomplish sending data, but failed to parse from JSON
      #p jdata.length
      #bmp = BitmapFactory.decodeByteArray(jdata, 0, jdata.length)

      #doSomethingNeeded(bmp);
      Thread.new(tmp2){ |jdata|
        #p jdata
        #p @activity
        @activity.client_instance.notify(:instruct, jdata)
      }
    end
	#send_to('TextCreator', @count)
	#faceDetector = new org.opencv.objdetect.CascadeClassifier(getClass().getResource("/lbpcascade_frontalface.xml").getPath());
    #Mat image = Highgui.imread(getClass().getResource("/lena.png").getPath());
  end
end