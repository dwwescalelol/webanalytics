import json
from github import Github
from pydantic import PrivateAttr
from dagster import ConfigurableResource

from data_ingestion.constants.table_names import Table


class GitHubPagesResource(ConfigurableResource):
    repo_name: str
    token: str
    _github: Github = PrivateAttr()
    _repo = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._github = Github(self.token)
        self._repo = self._github.get_repo(self.repo_name)

    def get_data(self, table: Table) -> list[dict]:
        contents = self._get_table_content(table)
        data = json.loads(contents.decoded_content.decode())
        return data

    def load_data(self, table: Table, new_data: list[dict], message: str):
        contents = self._get_table_content(table)
        updated_content = json.dumps(new_data, indent=2)
        self._repo.update_file(contents.path, message,
                               updated_content, contents.sha)


    def _get_table_content(self, table: Table):
        return self._repo.get_contents(f"docs/data/{table.value}.json")
