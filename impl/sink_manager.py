from utils.common import replace_letter, get_field_id, find_wellid
import pandas as pd
import logging
from utils.config import Config
from db.db_manager import DB
from sqlalchemy.orm import Session


class SinkManager:
    config: Config
    db: DB
    current_session: Session | None

    def __init__(self, config: Config, db: DB):
        self.config = config
        self.log = logging.getLogger('SinkManager')
        self.db = db

    def process_sinks(self, sinks):
        try:
            self.current_session = self.db.get_session()
            sinks = sinks[sinks['name'].str.startswith('SN_')]
            sinks['base_name'] = sinks['name'].apply(replace_letter)
            sinks['field_id'] = sinks['name'].apply(lambda row: get_field_id(row, self.config.field_ids))
            well_id_map = {f'{found_well.well_name}_{found_well.field_id}': found_well.wellid for found_well in
                           self.db.well_info_schema.get_well_info_by_well_name(list(sinks['base_name']),
                                                                               self.config.field_ids.values(), self.current_session)}
            sinks['wellid'] = sinks.apply(
                lambda row: find_wellid(row['base_name'], row['field_id'], well_id_map),
                axis=1)
            well_states_map = {well_state.wellid: well_state.w_state != self.config.in_work_state for well_state in
                               self.db.iss_dynamic_well_state_schema.get_well_states_by_wells_ids(sinks['wellid'],
                                                                                                  self.config.search_date, self.current_session)}
            sinks['w_state'] = sinks['wellid'].map(well_states_map)
            sinks['w_state'].fillna(True, inplace=True)
            active_sinks = sinks.loc[~sinks['w_state']]
            self.log.debug(f'Found {active_sinks.shape[0]} active sinks, starting getting data')
            dob_zak_data_map = {
                wellid: [obj for obj in self.db.iss_dob_zak_zak_tm_schema.get_iss_dob_zak_zak_tm(active_sinks['wellid'],
                                                                                                 self.config.search_date, self.current_session)
                         if
                         obj.wellid == wellid] for wellid in active_sinks['wellid']}
            active_sinks['qz_m3'] = active_sinks['wellid'].map(
                lambda wellid: round(self.__get_dob_zak_data(wellid, dob_zak_data_map), 2)
            )
            sinks = pd.merge(sinks, active_sinks, on=['wellid', 'name', 'base_name', 'w_state', 'field_id'], how='left')
            sinks['qz_m3'].fillna(0.01, inplace=True)
            sinks.loc[(sinks['qz_m3'] < 0.2) & (~sinks['w_state']), 'w_state'] = True
            self.log.debug('Finished getting sinks data')
            self.current_session = None
            return sinks
        except Exception as e:
            raise Exception('Error during process sinks') from e

    @staticmethod
    def __get_dob_zak_data(wellid, dob_zak_map):
        if wellid in dob_zak_map:
            sum_qz = sum(obj.qz_m3 for obj in dob_zak_map[wellid])
            return sum_qz if sum_qz != 0 else 0.01
        else:
            return 0.01
