from db.db_prepare import prepare_db
from impl.project_manager import ProjectManager

from utils.config import config_logging

prepare_db()
config_logging()
ProjectManager().process_projects()
