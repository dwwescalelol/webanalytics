from data_ingestion.constants.table_names import Table
from data_ingestion.resources.crazygames import CrazyGamesResource
from dagster import AssetIn, Output, asset, get_dagster_logger
import polars as pl
from dagster import asset

from data_ingestion.resources.github_pages_reasource import GitHubPagesResource




@asset(
    group_name="crazy_games",
    key_prefix="crazy_games",
)
def game_links(
    cg_resource: CrazyGamesResource,
) -> Output[pl.DataFrame]:
    responce = cg_resource.get_games()
    num_games = responce["games"]["data"]["total"]
    df = pl.DataFrame(responce["games"]["data"]["items"]).drop("videos")

    for i in range(2, int((num_games / 100) + 2)):
        new_page = pl.DataFrame(
            cg_resource.get_games(paginationPage=i)["games"]["data"]["items"]
        ).drop("videos")
        df = pl.concat([df, new_page])

    return Output(df, metadata={"Num Games": num_games})


@asset(
    group_name="crazy_games",
    key_prefix="crazy_games",
    ins={
        "game_links": AssetIn(key_prefix="crazy_games"),
    },
)
def game_ratings(
    cg_resource: CrazyGamesResource,
    game_links: pl.DataFrame,
) -> pl.DataFrame:
    df = game_links.with_columns(
        pl.col("slug").apply(
            cg_resource.get_game_rating).alias("ratings")
    )

    return df.with_columns(
        pl.col("ratings").struct.field("currentTime"),
        pl.col("ratings").struct.field("upVotes"),
        pl.col("ratings").struct.field("downVotes"),
    ).drop("ratings")


@asset(
    group_name="crazy_games",
    key_prefix="crazy_games",
    compute_kind="db",
    ins={
        "game_ratings": AssetIn(key_prefix="crazy_games"),
    },
)
def load_ratings(
    github_pages: GitHubPagesResource,
    game_ratings: pl.DataFrame,
) -> None:

    logger = get_dagster_logger()
    row = game_ratings.head(1)
    logger.info(row)
    logger.info(row.dtypes)
    row = row.row(0, named=True)
    logger.info(row)

    new_rating = {
        "id": row["id"],
        "name": row["name"],
        "up_votes": row["upVotes"],
        "down_votes": row["downVotes"],
        "currentTime": row["currentTime"]
    }

    # Fetch current data
    existing_data = github_pages.get_data(Table.GAME_RATINGS)

    # Append the new row
    existing_data.append(new_rating)

    # Update the JSON file on github_pages
    github_pages.load_data(Table.GAME_RATINGS, existing_data, f"Loaded {len(existing_data)} new rows.")

