import os
import json
from github import Github
from pydantic import BaseModel, PrivateAttr
from dagster import ConfigurableResource
from typing import List, Dict


class GitHubPagesResource(ConfigurableResource):
    repo_name: str
    token: str
    _github: Github = PrivateAttr()
    _repo = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._github = Github(self.token)
        self._repo = self._github.get_repo(self.repo_name)

    def get_data(self, path: str) -> List[Dict]:
        contents = self._repo.get_contents(path)
        data = json.loads(contents.decoded_content.decode())
        return data

    def update_data(self, path: str, new_data: List[Dict], message: str):
        contents = self._repo.get_contents(path)
        updated_content = json.dumps(new_data, indent=2)
        self._repo.update_file(contents.path, message,
                               updated_content, contents.sha)


# Usage example
if __name__ == "__main__":
    github_resource = GitHubPagesResource(
        repo_name="your-username/simple-json-db",
        token=os.getenv("GITHUB_TOKEN"),
    )
    github_resource.setup_for_execution(None)

    # Get data from a file
    game_ratings = github_resource.get_data("data/game_ratings.json")
    print(game_ratings)

    # Add a new row and update the file
    new_rating = {
        "id": 3,
        "name": "Game 3",
        "up_votes": 200,
        "down_votes": 15,
        "time": "2023-07-01T12:00:00"
    }
    game_ratings.append(new_rating)
    github_resource.update_data(
        "data/game_ratings.json", game_ratings, "Add new game rating")
