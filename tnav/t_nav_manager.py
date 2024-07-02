from tnav.t_nav_connection import snp_project
from tNavigator_python_API import ProjectType

nd_proj_list = snp_project.get_list_of_subprojects(type=ProjectType.ND)
