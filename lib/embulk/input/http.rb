Embulk::JavaPlugin.register_input(
  "http", "org.embulk.input.HttpInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
