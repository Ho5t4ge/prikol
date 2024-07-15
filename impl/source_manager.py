from utils.common import replace_letter, get_field_id, find_wellid
from db.schema import get_well_info_by_well_name, get_technology_well_by_wells_ids, \
    get_technology_well_by_parts_by_well_ids, \
    get_oil_density_by_field_id_and_stratum_id, get_well_states_by_wells_ids
from utils.config import field_ids
import pandas as pd


# sink и source
def calculate_gas_value(dob_data, technology_well):
    gas_factors = []
    for dob in dob_data:
        geological_characteristics = [x for x in
                                      get_oil_density_by_field_id_and_stratum_id(dob.field_id, dob.stratum_id) if
                                      x.density_oil_stratum is not None]
        if geological_characteristics:
            gas_factor = dob.gas_factor * geological_characteristics[0].density_oil_stratum
            if dob.rate_liquid_m3 is not None and technology_well.rate_liquid_m3 is not None:
                rate_liquid_persent = dob.rate_liquid_m3 / technology_well.rate_liquid_m3
                gas_factors.append({'gas_factor': gas_factor, 'rate_liquid_percent': rate_liquid_persent})
    weighted_gas_factors = [gas_factor['gas_factor'] * gas_factor['rate_liquid_percent'] for gas_factor in gas_factors]
    if len(gas_factors) != 0:
        return sum(weighted_gas_factors) / sum(gas_factor['rate_liquid_percent'] for gas_factor in gas_factors)
    else:
        return 0.0


def find_gas(wellid, technology_well_data_map):
    if wellid in technology_well_data_map:
        if technology_well_data_map.get(wellid).gas_factor == 0.0:
            dob_data = get_technology_well_by_parts_by_well_ids(wellid)
            return calculate_gas_value(dob_data, technology_well_data_map[wellid])
        else:
            data = technology_well_data_map.get(wellid)
            geological_characteristics = [x for x in
                                          get_oil_density_by_field_id_and_stratum_id(data.field_id, data.stratum_id) if
                                          x.density_oil_stratum is not None]
            if geological_characteristics:
                return data.gas_factor * geological_characteristics[0].density_oil_stratum
            else:
                return 0.0


def get_rate_liquid_m3(wellid, technology_well_data_map):
    if wellid in technology_well_data_map:
        return technology_well_data_map.get(wellid).rate_liquid_m3


def get_water_cut_m3(wellid, technology_well_data_map):
    if wellid in technology_well_data_map:
        return technology_well_data_map.get(wellid).water_cut_m3


def process_source(sources):
    sources = sources[~sources['name'].str.startswith('IN_')]
    sources['base_name'] = sources['name'].apply(replace_letter)
    sources['field_id'] = sources['name'].apply(get_field_id)
    found_wells = get_well_info_by_well_name(list(sources['base_name']), field_ids.values())
    well_id_map = {f'{found_well.well_name}_{found_well.field_id}': found_well.wellid for found_well in
                   found_wells}
    sources['wellid'] = sources.apply(
        lambda row: find_wellid(row['base_name'], row['field_id'], well_id_map),
        axis=1)
    well_states_map = {well_state.wellid: well_state.w_state != "В работе" for well_state in
                       get_well_states_by_wells_ids(sources['wellid'])}
    sources['w_state'] = sources['wellid'].map(well_states_map)
    sources['w_state'].fillna(True, inplace=True)

    filtered_sources = sources.loc[~sources['w_state']]
    technology_well_data_map = {technology_well.wellid: technology_well for technology_well in
                                get_technology_well_by_wells_ids(sources['wellid'])}
    filtered_sources['gas_factor_model'] = filtered_sources.apply(
        lambda row: find_gas(row['wellid'], technology_well_data_map), axis=1)
    filtered_sources['rate_liquid_m3'] = filtered_sources.apply(
        lambda row: get_rate_liquid_m3(row['wellid'], technology_well_data_map),
        axis=1)
    filtered_sources['water_cut_m3'] = filtered_sources.apply(
        lambda row: get_water_cut_m3(row['wellid'], technology_well_data_map),
        axis=1)
    merged_data = pd.merge(filtered_sources, sources, on='wellid', how='left')
    print(merged_data[merged_data['wellid'] == 44217])
