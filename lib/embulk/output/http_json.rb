Embulk::JavaPlugin.register_output(
  "http_json", "org.embulk.output.http_json.HttpJsonOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
