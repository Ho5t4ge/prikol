from db.db_prepare import prepare_db
import os
from utils.config import Config
from impl.project_manager import ProjectManager

config = Config(os.path.join(os.getcwd(), 'config.yaml'))
prepare_db()
ProjectManager(config).process_projects()
