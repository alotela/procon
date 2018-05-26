defmodule Procon.MixProject do
  use Mix.Project

  def project do
    [
      app: :procon,
      deps: deps(),
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      version: "0.1.0"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Procon.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 2.1"},
      {:erlkaf, git: "https://github.com/silviucpp/erlkaf.git", branch: :master},
      {:postgrex, ">= 0.0.0"}
    ]
  end
end
