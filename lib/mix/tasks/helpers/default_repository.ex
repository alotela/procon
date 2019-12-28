defmodule Mix.Tasks.Procon.Helpers.DefaultRepository do
  alias Mix.Tasks.Procon.Helpers
  import Mix.Generator

  def generate_repository(app_name, processor_name, processor_path, processor_repo) do
    repositories_path = Path.join([processor_path, "repositories"])
    Helpers.info("creating repository path #{repositories_path}")
    create_directory(repositories_path)

    repo_file_path = Path.join([repositories_path, "pg.ex"])

    unless File.exists?(repo_file_path) do
      Helpers.info("creating default repo file #{repo_file_path}")

      create_file(
        repo_file_path,
        pg_repo_template(
          app_name: app_name,
          processor_repo: Helpers.repo_name_to_module(processor_name, processor_repo)
        )
      )
    end
  end

  embed_template(:pg_repo, """
  defmodule <%= @processor_repo %> do
    use Ecto.Repo,
        otp_app: :<%= @app_name %>,
        adapter: Ecto.Adapters.Postgres
  end
  """)
end
