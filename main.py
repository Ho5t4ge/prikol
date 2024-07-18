#  Copyright (c) 2010-2024. LUKOIL-Engineering Limited KogalymNIPINeft Branch Office in Tyumen
#  Данным программным кодом владеет Филиал ООО "ЛУКОЙЛ-Инжиниринг" "КогалымНИПИнефть" в г.Тюмени

import os
from utils.config import Config
from impl.project_manager import ProjectManager
from db.db_manager import DB

config = Config(os.path.join(os.getcwd(), 'config.yaml'))
db = DB(config)
ProjectManager(config,db).process_projects()
