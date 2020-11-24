defmodule Procon.Helpers do
  def inspect(data, label \\ "", color \\ :red) do
    IO.inspect(
      data,
      label: label,
      limit: :infinity,
      syntax_colors: [
        atom: color,
        binary: color,
        boolean: color,
        list: color,
        map: color,
        nil: color,
        number: color,
        pid: color,
        regex: color,
        string: color,
        tuple: color
      ])
  end
end
