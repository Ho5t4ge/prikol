from utils.common import replace_letter, get_field_id, find_wellid
from db.schema import get_well_info_by_well_name, get_well_states_by_wells_ids, get_iss_dob_zak_zak_tm
import pandas as pd
import logging
from utils.config import Config


class SinkManager:
    config: Config

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('SinkManager')

    def process_sinks(self, sinks):
        try:
            sinks = sinks[sinks['name'].str.startswith('SN_')]
            sinks['base_name'] = sinks['name'].apply(replace_letter)
            sinks['field_id'] = sinks['name'].apply(lambda row: get_field_id(row, self.config.field_ids))
            well_id_map = {f'{found_well.well_name}_{found_well.field_id}': found_well.wellid for found_well in
                           get_well_info_by_well_name(list(sinks['base_name']), self.config.field_ids.values())}
            sinks['wellid'] = sinks.apply(
                lambda row: find_wellid(row['base_name'], row['field_id'], well_id_map),
                axis=1)
            well_states_map = {well_state.wellid: well_state.w_state != self.config.in_work_state for well_state in
                               get_well_states_by_wells_ids(sinks['wellid'], self.config.search_date)}
            sinks['w_state'] = sinks['wellid'].map(well_states_map)
            sinks['w_state'].fillna(True, inplace=True)
            active_sinks = sinks.loc[~sinks['w_state']]
            self.log.debug(f'Found {active_sinks.shape[0]} active sinks, starting getting data')
            dob_zak_data_map = {
                wellid: [obj for obj in get_iss_dob_zak_zak_tm(active_sinks['wellid'], self.config.search_date) if
                         obj.wellid == wellid] for wellid in active_sinks['wellid']}
            active_sinks['qz_m3'] = active_sinks['wellid'].map(
                lambda wellid: round(self.__get_dob_zak_data(wellid, dob_zak_data_map), 2)
            )
            sinks = pd.merge(sinks, active_sinks, on=['wellid', 'name', 'base_name', 'w_state', 'field_id'], how='left')
            sinks['qz_m3'].fillna(0.01, inplace=True)
            sinks.loc[(sinks['qz_m3'] < 0.2) & (~sinks['w_state']), 'w_state'] = True
            self.log.debug('Finished getting sinks data')
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
