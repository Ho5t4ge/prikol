from tNavigator_python_API import ProjectType

from db.db_prepare import prepare_db
from tnav.t_nav_manager import snp_project
from impl.source_manager import process_source
# sink и source

prepare_db()
wd_proj_list = snp_project.get_list_of_subprojects(type=ProjectType.ND)
for project in wd_proj_list:
    well_designer_project = snp_project.get_subproject_by_name(name=project, type=ProjectType.ND)
    well_designer_project.run_py_code(code="request_license_feature (feature='FEAT_NETWORK_DESIGNER')")
    sources = well_designer_project.run_py_code(code=""" 
import pandas as pd
sources=get_objects_by_type(type='source')
return pd.DataFrame(list(map(lambda source: {'name': source.name()}, sources)), columns=['name'])""")
    process_source(sources)


