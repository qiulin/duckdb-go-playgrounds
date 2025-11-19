package conf

type DuckDB struct {
	DataDir string `json:"data_dir"`
}

type Server struct {
	Http Http `json:"http"`
}

type Http struct {
	Addr string `json:"addr"`
}

type Config struct {
	Server Server `json:"server"`
	DuckDB DuckDB `json:"duckdb"`
}
