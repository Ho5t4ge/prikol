#  Copyright (c) 2010-2024. LUKOIL-Engineering Limited KogalymNIPINeft Branch Office in Tyumen
#  Данным программным кодом владеет Филиал ООО "ЛУКОЙЛ-Инжиниринг" "КогалымНИПИнефть" в г.Тюмени

from impl.source_manager import SourceManager
from impl.sink_manager import SinkManager
import logging
from tNavigator_python_API import ProjectType
from utils.config import Config
from db.db_manager import DB


class ProjectManager:
    config: Config
    source_manager: SourceManager
    sink_manager: SinkManager
    db: DB

    def __init__(self, config: Config, db: DB):
        self.config = config
        self.db = db
        self.source_manager = SourceManager(config, db)
        self.sink_manager = SinkManager(config, db)
        self.log = logging.getLogger('ProjectManager')

    def process_projects(self):
        self.log.debug('Starting process network projects')
        wd_proj_list = self.config.project.get_list_of_subprojects(type=ProjectType.ND)
        self.log.debug(f'Found {len(wd_proj_list)} projects')
        for project in wd_proj_list:
            try:
                self.log.debug(f'Starting process project with name {project}')
                well_designer_project = self.config.project.get_subproject_by_name(name=project, type=ProjectType.ND)
                well_designer_project.run_py_code(code="request_license_feature (feature='FEAT_NETWORK_DESIGNER')")
                sources = self.__get_objects(well_designer_project, 'source')
                if not sources.empty:
                    self.log.debug(f'Found {sources.shape[0]} sources, starting process')
                    sources = self.source_manager.process_source(sources)
                    self.__update_sources(well_designer_project, sources.to_dict(orient='list'))
                    self.log.debug('Updated sources in project')
                sinks = self.__get_objects(well_designer_project, 'sink')
                if not sinks.empty:
                    self.log.debug(f'Found {sinks.shape[0]} sinks, starting process')
                    sinks = self.sink_manager.process_sinks(sinks)
                    self.__update_sinks(well_designer_project, sinks.to_dict(orient='list'))
                    self.log.debug('Updated sinks in project')
                if sinks.empty & sources.empty:
                    self.log.warning(f"No found objects in project {project}")
                else:
                    wells = self.__get_objects(well_designer_project, 'well')
                    wells = wells.assign(w_state=True)
                    self.__update_wells(well_designer_project, wells.to_dict(orient='list'))
                    self.log.debug('Updated wells in project')
            except Exception as e:
                raise Exception(f'Error during process objects in project {project}') from e
        if self.config.project:
            self.log.debug("Closing project...")
            self.config.project.close_project()
            self.log.debug("Finished")

    def __get_objects(self, well_designer_project, object_type: str):
        self.log.debug(f'Starting getting {object_type} objects')
        try:
            return well_designer_project.run_py_code(code=f"""
import pandas as pd
objects=get_objects_by_type(type='{object_type}')
return pd.DataFrame(list(map(lambda object: {{'name': object.name()}}, objects)), columns=['name'])
    """)
        except Exception as e:
            raise Exception(f"Error during getting project objects with type {object_type}") from e

    def __update_sources(self, well_designer_project, sources):
        self.log.debug('Starting updating source objects')
        try:
            well_designer_project.run_py_code(code=f"""
import pandas as pd
from numpy import nan
df=pd.DataFrame({sources})
for index, row in df.iterrows():
    object_parameters_change (event_date=datetime (year={self.config.search_date.year},
         month={self.config.search_date.month},
         day={self.config.search_date.day},
         hour={self.config.search_date.hour},
         minute={self.config.search_date.minute},
         second={self.config.search_date.second}),
         index=find_nd_object (name=row['name'],
         type="source"),
         event_type="source_mixture_mass_rate",
         value=row['rate_liquid_m3'])
    nd_object_parameters_change_rate_type (event_date=datetime (year={self.config.search_date.year},
         month={self.config.search_date.month},
         day={self.config.search_date.day},
         hour={self.config.search_date.hour},
         minute={self.config.search_date.minute},
         second={self.config.search_date.second}),
         index=find_nd_object (name=row['name'],
         type="source"),
         event_type="source_rate_type",
         value="liquid_surface_rate")
    object_parameters_change (event_date=datetime (year={self.config.search_date.year},
         month={self.config.search_date.month},
         day={self.config.search_date.day},
         hour={self.config.search_date.hour},
         minute={self.config.search_date.minute},
         second={self.config.search_date.second}),
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
         event_date=datetime (year={self.config.search_date.year},
         month={self.config.search_date.month},
         day={self.config.search_date.day},
         hour={self.config.search_date.hour},
         minute={self.config.search_date.minute},
         second={self.config.search_date.second}))
    """)
        except Exception as e:
            raise Exception('Error during updating source objects') from e

    def __update_sinks(self, well_designer_project, sinks):
        self.log.debug('Starting updating sink objects')
        try:
            well_designer_project.run_py_code(code=f"""
import pandas as pd
from numpy import nan
df = pd.DataFrame({sinks})
for index, row in df.iterrows():
    nd_object_parameters_change_rate_type(event_date=datetime(year={self.config.search_date.year},
      month={self.config.search_date.month},
      day={self.config.search_date.day},
      hour={self.config.search_date.hour},
      minute={self.config.search_date.minute},
      second={self.config.search_date.second}),
      index=find_nd_object(name=row['name'],
      type="sink"),
      event_type="sink_rate_type",
      value="liquid_surface_rate")
    object_parameters_change(event_date=datetime(year={self.config.search_date.year},
      month={self.config.search_date.month},
      day={self.config.search_date.day},
      hour={self.config.search_date.hour},
      minute={self.config.search_date.minute},
      second={self.config.search_date.second}),
      index=find_nd_object(name=row['name'],
      type="sink"),
      event_type="status",
      value=row['w_state'])
    object_parameters_change(event_date=datetime(year={self.config.search_date.year},
      month={self.config.search_date.month},
      day={self.config.search_date.day},
      hour={self.config.search_date.hour},
      minute={self.config.search_date.minute},
      second={self.config.search_date.second}),
      index=find_nd_object(name=row['name'],
      type="sink"),
      event_type="sink_mixture_mass_rate",
      value=row['qz_m3'])
""")
        except Exception as e:
            raise Exception("Error during updating sink objects") from e

    def __update_wells(self, well_designer_project, wells):
        self.log.debug('Starting updating wells objects')
        try:
            well_designer_project.run_py_code(code=f"""
import pandas as pd
from numpy import nan
df = pd.DataFrame({wells})
for index, row in df.iterrows():
    object_parameters_change(event_date=datetime(year={self.config.search_date.year},
      month={self.config.search_date.month},
      day={self.config.search_date.day},
      hour={self.config.search_date.hour},
      minute={self.config.search_date.minute},
      second={self.config.search_date.second}),
      index=find_nd_object(name=row['name'],
      type="well"),
      event_type="status",
      value=row['w_state'])
""")
        except Exception as e:
            raise Exception("Error during updating wells objects") from e
