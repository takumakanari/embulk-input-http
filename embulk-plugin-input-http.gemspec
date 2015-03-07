# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "embulk-plugin-input-http"
  spec.version       = "0.0.1"
  spec.authors       = ["Takuma kanari"]
  spec.email         = ["chemtrails.t@gmail.com"]
  spec.summary       = %q{Embulk plugin for http input}
  spec.description   = %q{fetch data via http}
  spec.homepage      = "https://github.com/takumakanari/embulk-plugin-input-http"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "jsonpath", "~> 0.5"
  spec.add_development_dependency "bundler", "~> 1.0"
  spec.add_development_dependency "rake", "~> 0.9.2"
end
