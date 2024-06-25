import os
from dagster import Definitions, load_assets_from_modules
from data_ingestion.assets import crazy_games
from data_ingestion.resources.crazygames import CrazyGamesResource
from data_ingestion.resources.io_managers import LocalFileSystemIOManager
from data_ingestion.resources.github_pages_reasource import GitHubPagesResource

deployment_env = os.getenv("DEPLOYMENT_ENV", "dev")

all_assets = load_assets_from_modules([crazy_games])
print(f"Loaded assets: {all_assets}")

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": LocalFileSystemIOManager(base_path="dagster_home/data"),
        "cg_resource": CrazyGamesResource(),
        "github_pages": GitHubPagesResource(
            repo_name="dwwescalelol/webanalytics",
            token=os.getenv("GITHUB_TOKEN")
        )
    },
)
