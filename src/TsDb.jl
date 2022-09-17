module TsDb

using YAML

export Client

const path_tsdb = YAML.load_file(".\\src\\config.yaml")["path_tsdb"]
include("client.jl")

# precompile()

end  # module TsDb
