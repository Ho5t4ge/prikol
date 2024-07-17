class SessionException(Exception):
    def __init__(self):
        super().__init__("Not configured session!")
