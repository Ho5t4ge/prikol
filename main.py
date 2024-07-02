from tNavigator_python_API import ProjectType

from db.db_prepare import prepare_db
from tnav.t_nav_manager import snp_project
from utils.utils import replace_letter
from db.schema.well_info import get_well_info_by_well_name

# sink и source
prepare_db()
wd_proj_list = snp_project.get_list_of_subprojects(type=ProjectType.ND)
for project in wd_proj_list:
    well_designer_project = snp_project.get_subproject_by_name(name=project, type=ProjectType.ND)
    well_designer_project.run_py_code(code="request_license_feature (feature='FEAT_NETWORK_DESIGNER')")
    wells = well_designer_project.run_py_code(code="""
import pandas as pd
wells=get_objects_by_type(type='well')
return pd.DataFrame(list(map(lambda well: {'name': well.name()}, wells)), columns=['name'])""")
    wells['name'] = wells['name'].apply(replace_letter)
    well_names = wells['name'].values.tolist()
    found_wells = get_well_info_by_well_name(well_names)
    print(found_wells)
