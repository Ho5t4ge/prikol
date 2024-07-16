from utils.common import replace_letter, get_field_id, find_wellid
from db.schema import get_well_info_by_well_name, get_well_states_by_wells_ids, get_iss_dob_zak_zak_tm
from utils.config import field_ids, in_work_state
import pandas as pd
import logging

log = logging.getLogger('SinkManager')

def get_dob_zak_data(wellid, dob_zak_map):
    if wellid in dob_zak_map:
        sum_qz=sum(obj.qz_m3 for obj in dob_zak_map[wellid])
        return sum_qz if sum_qz != 0 else 0.01
    else:
        return 0.0


def process_sinks(sinks):
    try:
        sinks = sinks[sinks['name'].str.startswith('SN_')]
        sinks['base_name'] = sinks['name'].apply(replace_letter)
        sinks['field_id'] = sinks['name'].apply(get_field_id)
        well_id_map = {f'{found_well.well_name}_{found_well.field_id}': found_well.wellid for found_well in
                       get_well_info_by_well_name(list(sinks['base_name']), field_ids.values())}
        sinks['wellid'] = sinks.apply(
            lambda row: find_wellid(row['base_name'], row['field_id'], well_id_map),
            axis=1)
        well_states_map = {well_state.wellid: well_state.w_state != in_work_state for well_state in
                           get_well_states_by_wells_ids(sinks['wellid'])}
        sinks['w_state'] = sinks['wellid'].map(well_states_map)
        sinks['w_state'].fillna(True, inplace=True)
        active_sinks = sinks.loc[~sinks['w_state']]
        log.debug(f'Found {active_sinks.shape[0]} active sinks, starting getting data')
        dob_zak_data_map = {wellid: [obj for obj in get_iss_dob_zak_zak_tm(active_sinks['wellid']) if obj.wellid == wellid] for wellid in
                            active_sinks['wellid']}
        active_sinks['qz_m3'] = active_sinks['wellid'].map(
            lambda wellid: round(get_dob_zak_data(wellid, dob_zak_data_map), 2)
        )
        sinks = pd.merge(sinks, active_sinks, on=['wellid', 'name', 'base_name', 'w_state', 'field_id'], how='left')
        sinks['qz_m3'].fillna(0.01, inplace=True)
        sinks.loc[(sinks['qz_m3'] == 0.0) & (sinks['w_state'] == False), 'w_state'] = True
        log.debug('Finished getting sinks data')
        return sinks
    except Exception as e:
        raise Exception(f'Error during process sinks : {e}')
