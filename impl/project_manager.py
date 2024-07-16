from tnav.t_nav_manager import snp_project
from impl.source_manager import process_source
from impl.sink_manager import process_sinks
import logging
from tNavigator_python_API import ProjectType
from utils.config import search_date

log = logging.getLogger('ProjectManager')


class ProjectManager:
    @staticmethod
    def process_projects():
        log.debug('Starting process network projects')
        wd_proj_list = snp_project.get_list_of_subprojects(type=ProjectType.ND)
        log.debug(f'Found {len(wd_proj_list)} projects')
        for project in wd_proj_list:
            try:
                if project == 'Тест':
                    continue
                log.debug(f'Starting process project with name {project}')
                well_designer_project = snp_project.get_subproject_by_name(name=project, type=ProjectType.ND)
                well_designer_project.run_py_code(code="request_license_feature (feature='FEAT_NETWORK_DESIGNER')")
                sources = get_objects(well_designer_project, 'source')
                if ~sources.empty:
                    log.debug(f'Found {sources.shape[0]} sources, starting process')
                    sources = process_source(sources)
                    update_sources(well_designer_project, sources.to_dict(orient='list'))
                    log.debug(f'Updated sources in project with name {project}')
                sinks = get_objects(well_designer_project, 'sink')
                log.debug(f'Found {sinks.shape[0]} sinks, starting process')
                if ~sinks.empty:
                    log.debug(f'Found {sinks.shape[0]} sinks, starting process')
                    sinks = process_sinks(sinks)
                    update_sinks(well_designer_project, sinks.to_dict(orient='list'))
                    log.debug(f'Updated sinks in project with name {project}')
                if sinks.empty & sources.empty:
                    log.warning(f"No found objects in project with name{project}")
            except Exception as e:
                raise Exception(f'Error during process objects in project {project}, Error message: {e}')
        if snp_project:
            log.debug("Closing project...")
            snp_project.close_project()


def get_objects(well_designer_project, object_type: str):
    log.debug(f'Starting getting {object_type} objects')
    try:
        return well_designer_project.run_py_code(code=f"""
import pandas as pd
objects=get_objects_by_type(type='{object_type}')
return pd.DataFrame(list(map(lambda object: {{'name': object.name()}}, objects)), columns=['name'])
""")
    except Exception as e:
        raise Exception(f"Error during getting project objects with type {object_type}. Error message:{e}")


def update_sources(well_designer_project, sources):
    log.debug(f'Starting updating source objects')
    try:
        well_designer_project.run_py_code(code=f"""
import pandas as pd
from numpy import nan
df=pd.DataFrame({sources})
for index, row in df.iterrows():
    object_parameters_change (event_date=datetime (year={search_date.year},
         month={search_date.month},
         day={search_date.day},
         hour={search_date.hour},
         minute={search_date.minute},
         second={search_date.second}),
         index=find_nd_object (name=row['name'],
         type="source"),
         event_type="source_mixture_mass_rate",
         value=row['rate_liquid_m3'])
    nd_object_parameters_change_rate_type (event_date=datetime (year={search_date.year},
         month={search_date.month},
         day={search_date.day},
         hour={search_date.hour},
         minute={search_date.minute},
         second={search_date.second}),
         index=find_nd_object (name=row['name'],
         type="source"),
         event_type="source_rate_type",
         value="liquid_surface_rate")
    object_parameters_change (event_date=datetime (year={search_date.year},
         month={search_date.month},
         day={search_date.day},
         hour={search_date.hour},
         minute={search_date.minute},
         second={search_date.second}),
         index=find_nd_object (name=row['name'],
         type="source"),
         event_type="status",
         value=row['w_state'])
    nd_object_adjust_surface_volume_rate (object=find_nd_object (name=row['name'],
         type="source"),
         hydrocarbon_param="GOR",
         hydrocarbon_value=row['gas_factor_model'],
         water_param="WCUT",
         water_value=row['water_cut_m3']/100,
         event_date=datetime (year={search_date.year},
         month={search_date.month},
         day={search_date.day},
         hour={search_date.hour},
         minute={search_date.minute},
         second={search_date.second}))
""")
    except Exception as e:
        raise Exception(f'Error during updating source objects. Error message:{e}')


def update_sinks(well_designer_project, sinks):
    log.debug(f'Starting updating sink objects')
    try:
        well_designer_project.run_py_code(code=f"""
import pandas as pd
from numpy import nan

df = pd.DataFrame({sinks})
for index, row in df.iterrows():
    nd_object_parameters_change_rate_type(event_date=datetime(year={search_date.year},
      month={search_date.month},
      day={search_date.day},
      hour={search_date.hour},
      minute={search_date.minute},
      second={search_date.second}),
      index=find_nd_object(name=row['name'],
      type="sink"),
      event_type="sink_rate_type",
      value="liquid_surface_rate")
    object_parameters_change(event_date=datetime(year={search_date.year},
      month={search_date.month},
      day={search_date.day},
      hour={search_date.hour},
      minute={search_date.minute},
      second={search_date.second}),
      index=find_nd_object(name=row['name'],
      type="sink"),
      event_type="status",
      value=row['w_state'])
    object_parameters_change(event_date=datetime(year={search_date.year},
      month={search_date.month},
      day={search_date.day},
      hour={search_date.hour},
      minute={search_date.minute},
      second={search_date.second}),
      index=find_nd_object(name=row['name'],
      type="sink"),
      event_type="sink_mixture_mass_rate",
      value=row['qz_m3'])
""")
    except Exception as e:
        raise Exception(f"Error during updating sink objects. Error message:{e}")
