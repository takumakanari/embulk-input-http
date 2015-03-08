require "net/http"
require "uri"
require "bracecomp"

module Embulk
  module Input

    class HttpInputPlugin < InputPlugin
      Plugin.register_input("http", self)

      def self.transaction(config, &control)
        task = {
          :url => config.param("url", :string),
          :method => config.param("method", :string, default: "get"),
          :schema => config.param("schema", :array),
          :iterate => config.param("iterate", :hash),
          :open_timeout => config.param("open_timeout", :float, default: 2.0),
          :read_timeout => config.param("read_timeout", :float, default: 10.0),
          :done => config.param("done", :array, default: [])
        }
        params = config.param("params", :array, default: [])
        params_unexpand, params_expand = configure_queries(params)

        data_type = task[:iterate]["type"]
        unless ["json", "xml"].include?(data_type)
          raise "Unknown data_type #{data_type}, only supported for json or xml"
        end

        columns = task[:schema].each_with_index.map do |c, i|
          Column.new(i, c["name"], c["type"].to_sym)
        end

        task[:params] = params_unexpand
        task[:params_expand] = params_expand - task[:done]
        num_of_threads = task[:params_expand].empty? ? 1 : task[:params_expand].size

        report = yield(task, columns, num_of_threads)
        {"done" => report.map{|r| r["done"]}.compact}
      end

      def self.configure_queries(params)
        base = params.select{|p| !p["expand"]}.map do |p|
          [p["name"], p["value"]]
        end
        expands = params.select{|p| p["expand"] }.map do |p|
          p["value"].expand.map do |v|
            [p["name"], v]
          end
        end
        if expands.size > 0
            dest = expands.first.product(*(expands.slice(1, expands.size - 1)))
            dest.sort!{|a, b| "#{a[0]}=#{a[1]}" <=> "#{b[0]}=#{b[1]}"}
        else
            dest = []
        end
        [base, dest]
      end

      def run
        schema = @task["schema"]
        iterate = @task["iterate"]
        url = @task["url"]
        method = @task["method"]

        params_expand = @task["params_expand"][@index] || []
        query = URI.encode_www_form(@task["params"] + params_expand)
        puts "#{@index}: #{method.upcase} #{url}?#{query}"

        data = fetch(url, method, query).body
        data_type = iterate["type"]

        case data_type
          when "json"
            iter = IterJson.new(schema, data, iterate)
          when "xml"
            iter = IterXML.new(schema, data, iterate)
          else
            raise "Unsupported data_type #{data_type}"
        end

        iter.each do |record|
          @page_builder.add(record)
        end
        @page_builder.finish

        {:done => params_expand}
      end

      private

      def fetch(url, method, query)
        uri = URI.parse(url)

        res = Net::HTTP.start(uri.host, uri.port) do |client|
          client.open_timeout = @task["open_timeout"]
          client.read_timeout = @task["read_timeout"]
          case method.downcase
            when "get"
              client.get([uri.path, query].join("?"))
            when "post"
              client.post(uri.path, query)
            else
              raise "Unsupported method #{method}"
          end
        end

        case res
          when Net::HTTPSuccess
            res
          else
            raise "Request is not successful, code=#{res.code}, value=#{res.body}"
        end
      end

      class Iter
        def initialize(schema, data, config)
          @schema = schema
          @data = data
          @config = config
        end

        def each
          raise NotImplementedError.new("each")
        end

        private

        def make_record(e)
          @schema.map do |c|
            name = c["name"]
            path = c["path"]
            val = path.nil? ? e[name] : find_by_path(e, path)

            v = val.nil? ? "" : val
            type = c["type"]
            case type
              when "string"
                v
              when "long"
                v.to_i
              when "double"
                v.to_f
              when "boolean"
                ["yes", "true", "1"].include?(v)
              when "timestamp"
                v.empty? ? nil : Time.strptime(v, c["format"])
              else
                raise "Unsupported type #{type}"
            end
          end
        end

        def find_by_path(e, path)
          raise NotImplementedError.new("Find by path is unsupported")
        end
      end

      class IterXML < Iter
        def initialize(schema, data, config)
          require "rexml/document"
          super
          @doc = REXML::Document.new(@data)
        end

        def each
          @doc.elements.each(@config["path"]) do |e|
            dest = {}
            e.elements.each do |d|
              dest[d.name] = d.text
            end
            yield make_record(dest)
          end
        end
      end

      class IterJson < Iter
        def initialize(schema, data, config)
          require "jsonpath"
          super
          @jsonpath = JsonPath.new(@config["path"])
        end

        def each
          @jsonpath.on(@data).flatten.each do |e|
            yield make_record(e)
          end
        end

        def find_by_path(e, path)
          JsonPath.on(e, path).first
        end
      end

    end
  end
end
