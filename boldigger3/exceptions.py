# build a custom exception to handle bad responses from BOLD
class DownloadFinished(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
