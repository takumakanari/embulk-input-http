require "net/http"
require "uri"

module Embulk
  module Input

    class HttpInputPlugin < InputPlugin
      Plugin.register_input("http", self)

      def self.transaction(config, &control)
        url = config.param("url", :string)
        schema = config.param("schema", :array)
        method = config.param("method", :string, default: "get")
        params = config.param("params", :array, default: [])
        iterate = config.param("iterate", :hash)
        open_timeout = config.param("open_timeout", :float, default: 2.0)
        read_timeout = config.param("read_timeout", :float, default: 10.0)

        data_type = iterate["type"]
        unless ["json", "xml"].include?(data_type)
          raise "Unknown data_type #{data_type}, only supported for json or xml"
        end

        columns = schema.each_with_index.map do |c, i|
          Column.new(i, c["name"], c["type"].to_sym)
        end

        task = {
          :url => url,
          :method => method,
          :params => params,
          :schema => schema,
          :iterate => iterate,
          :open_timeout => open_timeout,
          :read_timeout => read_timeout
        }

        report = yield(task, columns, 1)
        config.merge(report["done"].flatten.compact)
        {}
      end

      def run
        schema = @task["schema"]
        iterate = @task["iterate"]

        data = fetch.body

        case iterate["type"]
        when "json"
          iter = IterJson.new(data, iterate["path"])
        when "xml"
          iter = IterXML.new(data, iterate["path"])
        else
          raise "Unsupported data_type #{data_type}"
        end

        rows = 0
        iter.each do |e|
          rows += 1
          @page_builder.add(schema.map{|c|
            name = c["name"]
            type = c["type"]
            val = e[name].nil? ? "" : e[name]
            case type
            when "string"
              val
            when "long"
              val.to_i
            when "double"
              val.to_f
            when "boolean"
              ["yes", "true", "1"].include?(val)
            when "timestamp"
              if val.nil? || val.empty?
                nil
              else
                Time.strptime(val, c["format"])
              end
            else
              raise "Unsupported type #{type}"
            end
          })
        end
        @page_builder.finish

        {:rows => rows}
      end

      private

      def fetch
        uri = URI.parse(@task["url"])
        method = @task["method"]
        qs = URI.encode_www_form(@task["params"].inject({}) {|memo, p|
          memo[p["name"]] = p["value"]
          memo
        })
        puts "#{method.upcase} #{uri}?#{qs}"

        res = Net::HTTP.start(uri.host, uri.port) do |client|
          client.open_timeout = @task["open_timeout"]
          client.read_timeout = @task["read_timeout"]
          case method.downcase
          when "get"
            client.get([uri.path, qs].join("?"))
          when "post"
            client.post(uri.path, qs)
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
        def initialize(data, path)
          @data = data
          @path = path
        end

        def each
          raise NotImplementedError("each")
        end
      end

      class IterXML < Iter
        def initialize(data, path)
          require "rexml/document"
          super
          @doc = REXML::Document.new(@data)
        end

        def each
          @doc.elements.each(@path) do |e|
            ret = {}
            e.elements.each do |d|
              ret[d.name] = d.text
            end
            yield ret
          end
        end
      end

      class IterJson < Iter
        def initialize(data, path)
          require "jsonpath"
          super
          @jsonpath = JsonPath.new(@path)
        end

        def each
          @jsonpath.on(@data).flatten.each do |e|
            raise "data is must be hash, but #{e.class}" unless e.instance_of?(Hash)
            yield e
          end
        end
      end

    end
  end
end
