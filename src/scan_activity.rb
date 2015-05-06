require 'ruboto/activity'
require 'ruboto/widget'
require 'ruboto/util/toast'
require 'detector'
require 'preview_callback'
require_relative 'service_node'

import android.util.Log
import android.view.Surface
import android.view.WindowManager


java_import "javax.microedition.khronos.egl.EGL10"
java_import "javax.microedition.khronos.egl.EGLConfig"
java_import "javax.microedition.khronos.opengles.GL10"

java_import "java.nio.ByteBuffer"
java_import "java.nio.ByteOrder"
java_import "java.nio.IntBuffer"

ruboto_import_widgets :Button, :LinearLayout, :FrameLayout, :ScrollView, :TextView
ruboto_import_widget :SurfaceView, "android.view"
ruboto_import_widget :GLSurfaceView, "android.opengl"

class CamdetectActivity
	attr_reader :client_instance
  def on_create(bundle)
    super
    puts "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
    Thread.with_large_stack(512) do
      ServiceNode.instance
      num = ServiceNode.instance.establish_client_modules self
      puts "Established #{num} modules locally."
      sleep 10 #if num < 5
      @client_instance = ServiceNode.instance.run_app 'BuildingDetection'
    end

    rotation = {
        Surface::ROTATION_0 => 0,Surface::ROTATION_90 => 90,Surface::ROTATION_180 => 180,Surface::ROTATION_270 => 270
    }[window_manager.default_display.rotation]#[Surface::ROTATION_90]
    #self.title = "Camdetect #{rotation}"
    #self.setRequestedOrientation(android.content.pm.ActivityInfo::SCREEN_ORIENTATION_LANDSCAPE)
    window.add_flags(WindowManager::LayoutParams::FLAG_KEEP_SCREEN_ON)



		@renderer = RubotoGLSurfaceViewRenderer.new	
		setContentView(linear_layout(:orientation => :vertical) do
			#linear_layout do
				#  text_view :text => "Detection status: "
				#  @server_status_view = text_view
				#end
				#linear_layout do
				#  text_view :text => "Detection result: "
				#  @camera_status_view = text_view
				#end
			frame_layout(:layout => {:width => :fill_parent, :height => :fill_parent}) do
				sv = surface_view
				@surface_holder_callback = RubotoSurfaceHolderCallback.new(self, rotation)
				sv.holder.add_callback @surface_holder_callback
				# Deprecated, but still required for older API version
				sv.holder.set_type android.view.SurfaceHolder::SURFACE_TYPE_PUSH_BUFFERS	
=begin
				#@surface_view = g_l_surface_view :renderer => @renderer, 
				#						         :on_click_listener => (proc{@renderer.change_angle})
				@surface_view = g_l_surface_view
				@surface_view.setZOrderOnTop(true)
				#@surface_view.setEGLContextClientVersion(2)
					@surface_view.setEGLConfigChooser(8, 8, 8, 8, 16, 0)
				@surface_view.holder.setFormat(android.graphics.PixelFormat::TRANSLUCENT)
				#@surface_view.setRenderMode(android.opengl.GLSurfaceView::RENDERMODE_WHEN_DIRTY)
				@surface_view.setRenderer(@renderer)
				@surface_view.setOnClickListener(proc{@renderer.change_angle})
=end
				linear_layout(:orientation => :vertical) do
					linear_layout do
						text_view :text => 'Detection FPS: ', :setTextColor => android.graphics.Color::GREEN
            @server_status_view = text_view
            @server_status_view.setTextColor(android.graphics.Color::GREEN)
          end

=begin
					linear_layout do
						text_view :text => "Result of frame ", :setTextColor => android.graphics.Color::GREEN
						@camera_status_view = text_view
            @camera_status_view.setTextColor(android.graphics.Color::GREEN)
					end
=end
				end		
			end
	  
#	  linear_layout(:layout => {:width => :fill_parent, :height => :fill_parent}) do
#		  sv = surface_view
#		  @surface_holder_callback = RubotoSurfaceHolderCallback.new(self, rotation)
#		  sv.holder.add_callback @surface_holder_callback
#		  # Deprecated, but still required for older API version
#		  sv.holder.set_type android.view.SurfaceHolder::SURFACE_TYPE_PUSH_BUFFERS		
#	  end

 
		end)

  end
  
  def camera_status=(value)
    run_on_ui_thread { @camera_status_view.text = value }
  end

  def server_status=(value)
    run_on_ui_thread { @server_status_view.text = value }
  end

  def toast=(str)
    run_on_ui_thread { toast str }
  end
end

class RubotoSurfaceHolderCallback
  def initialize(activity, rotation)
    @activity = activity
    @rotation = rotation
  end

  def surfaceCreated(holder)
    puts 'RubotoSurfaceHolderCallback#surfaceCreated'
    @camera = android.hardware.Camera.open
    parameters = @camera.parameters
    parameters.picture_format = android.graphics.PixelFormat::JPEG
    parameters.rotation = (360 + (90 - @rotation)) % 360
    parameters.set_focus_mode(android.hardware.Camera::Parameters::FOCUS_MODE_CONTINUOUS_VIDEO)
    parameters.set_preview_size(1280,720)#(768, 432)
    #parameters.set_picture_size(64, 32)#(640, 480)
    @camera.parameters = parameters
    @camera.preview_display = holder
    @camera.display_orientation = (360 + (90 - @rotation)) % 360
    @camera.start_preview
		@preview_callback = PreviewCallback.new(@activity)
    @camera.set_preview_callback @preview_callback
		@camera.start_preview
    #Detector.start(@activity, @camera)
  end

  def surfaceChanged(holder, format, width, height)
  end

  def surfaceDestroyed(holder)
    #Detector.stop
    @camera.stop_preview
    @camera.release
    @camera = nil
  end
end

class Cube
  def initialize
    one = 0x10000
    vertices = [
      -one, -one, -one,
       one, -one, -one,
       one, one, -one,
      -one, one, -one,
      -one, -one, one,
       one, -one, one,
       one, one, one,
      -one, one, one,
    ]

    colors = [
        0, 0, 0, one,
      one, 0, 0, one,
      one, one, 0, one,
        0, one, 0, one,
        0, 0, one, one,
      one, 0, one, one,
      one, one, one, one,
        0, one, one, one,
     ]

    indices = [
      0, 4, 5, 0, 5, 1,
      1, 5, 6, 1, 6, 2,
      2, 6, 7, 2, 7, 3,
      3, 7, 4, 3, 4, 0,
      4, 7, 6, 4, 6, 5,
      3, 0, 1, 3, 1, 2
    ]

    vbb = ByteBuffer.allocateDirect(vertices.length*4)
    vbb.order(ByteOrder.nativeOrder)
    @vertex_buffer = vbb.asIntBuffer
    @vertex_buffer.put(vertices.to_java(:int))
    @vertex_buffer.position(0)

    cbb = ByteBuffer.allocateDirect(colors.length*4)
    cbb.order(ByteOrder.nativeOrder)
    @color_buffer = cbb.asIntBuffer
    @color_buffer.put(colors.to_java(:int))
    @color_buffer.position(0)

    @index_buffer = ByteBuffer.allocateDirect(indices.length)
    @index_buffer.put(indices.to_java(:byte))
    @index_buffer.position(0)
  end

  def draw(gl)
    gl.glFrontFace(GL10::GL_CW)
    gl.glVertexPointer(3, GL10::GL_FIXED, 0, @vertex_buffer)
    gl.glColorPointer(4, GL10::GL_FIXED, 0, @color_buffer)
    gl.glDrawElements(GL10::GL_TRIANGLES, 36, GL10::GL_UNSIGNED_BYTE, @index_buffer)
  end
end

class RubotoGLSurfaceViewRenderer
  def initialize
    @translucent_background = true
    @cube = Cube.new
    @angle = 0.0
    @offset = 1.2
  end

  def onDrawFrame(gl)
    gl.glClear(GL10::GL_COLOR_BUFFER_BIT | GL10::GL_DEPTH_BUFFER_BIT)

    gl.glMatrixMode(GL10::GL_MODELVIEW)
    gl.glLoadIdentity
    gl.glTranslatef(0, 0, -3.0)
    gl.glRotatef(@angle, 0, 1, 0)
    gl.glRotatef(@angle*0.25, 1, 0, 0)

    gl.glEnableClientState(GL10::GL_VERTEX_ARRAY)
    gl.glEnableClientState(GL10::GL_COLOR_ARRAY)

    @cube.draw(gl)

    gl.glRotatef(@angle*2.0, 0, 1, 1)
    gl.glTranslatef(0.5, 0.5, 0.5)

    @cube.draw(gl)
    @angle += @offset
  end

  def onSurfaceChanged(gl, width, height)
    gl.glViewport(0, 0, width, height)
    ratio = width.to_f / height.to_f
    gl.glMatrixMode(GL10::GL_PROJECTION)
    gl.glLoadIdentity
    gl.glFrustumf(-ratio, ratio, -1, 1, 1, 10)
  end

  def onSurfaceCreated(gl, config)
    gl.glDisable(GL10::GL_DITHER)

    gl.glHint(GL10::GL_PERSPECTIVE_CORRECTION_HINT, GL10::GL_FASTEST)

    if (@translucent_background)
      gl.glClearColor(0,0,0,0)
    else
      gl.glClearColor(1,1,1,1)
    end
    gl.glEnable(GL10::GL_CULL_FACE)
    gl.glShadeModel(GL10::GL_SMOOTH)
    gl.glEnable(GL10::GL_DEPTH_TEST)
  end

  def change_angle
    @offset = -@offset
  end
end