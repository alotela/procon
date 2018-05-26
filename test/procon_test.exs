defmodule ProconTest do
  use ExUnit.Case
  doctest Procon

  test "greets the world" do
    assert Procon.hello() == :world
  end
end
