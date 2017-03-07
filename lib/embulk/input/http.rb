Embulk::JavaPlugin.register_input(
  "http", "org.embulk.input.http.HttpFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
