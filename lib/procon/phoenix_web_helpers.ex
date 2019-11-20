defmodule Procon.PhoenixWebHelpers do
  # used in generated "ProcessorWeb" file
  # procon processor's equivalent of Phoenix "YourPojectWeb" module
  # define custom layout and views paths in processor's directory
  def module_to_view(controller_module) do
    controller_module
    |> to_string()
    |> String.replace("Controllers", "Views")
    |> String.to_atom()
  end
end
