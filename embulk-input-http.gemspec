
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-http"
  spec.version       = "0.0.17"
  spec.authors       = ["Takuma kanari"]
  spec.email         = ["chemtrails.t@gmail.com"]
  spec.summary       = %q{Embulk plugin for http input}
  spec.description   = %q{Fetch data via http}
  spec.homepage      = "https://github.com/takumakanari/embulk-input-http"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
