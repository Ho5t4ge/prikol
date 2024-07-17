from utils.common import replace_letter, get_field_id, find_wellid
import pandas as pd
import logging
from utils.config import Config
from db.db_manager import DB


class SourceManager:
    config: Config
    db: DB

    def __init__(self, config: Config, db: DB):
        self.config = config
        self.log = logging.getLogger('SourceManager')
        self.db = db

    def process_source(self, sources):
        try:
            sources = sources[~sources['name'].str.startswith('IN_')]
            sources['base_name'] = sources['name'].apply(replace_letter)
            sources['field_id'] = sources['name'].apply(lambda row: get_field_id(row, self.config.field_ids))
            well_id_map = {f'{found_well.well_name}_{found_well.field_id}': found_well.wellid for found_well in
                           self.db.well_info_schema.get_well_info_by_well_name(list(sources['base_name']),
                                                                               self.config.field_ids.values())}
            sources['wellid'] = sources.apply(
                lambda row: find_wellid(row['base_name'], row['field_id'], well_id_map),
                axis=1)
            well_states_map = {well_state.wellid: well_state.w_state != self.config.in_work_state for well_state in
                               self.db.iss_dynamic_well_state_schema.get_well_states_by_wells_ids(sources['wellid'],
                                                                                                  self.config.search_date)}
            sources['w_state'] = sources['wellid'].map(well_states_map)
            sources['w_state'].fillna(True, inplace=True)
            active_sources = sources.loc[~sources['w_state']]
            self.log.debug(f'Found {active_sources.shape[0]} active sources, starting getting data')
            technology_well_data_map = {technology_well.wellid: technology_well for technology_well in
                                        self.db.technology_well_schema.get_technology_well_by_wells_ids(
                                            active_sources['wellid'],
                                            self.config.search_date)}
            active_sources['gas_factor_model'] = active_sources.apply(
                lambda row: self.__find_gas(row['wellid'], technology_well_data_map), axis=1)
            self.log.debug('Starting calculate weighted average gas factor')
            active_sources['rate_liquid_m3'] = active_sources.apply(
                lambda row: self.__get_rate_liquid_m3(row['wellid'], technology_well_data_map),
                axis=1)
            active_sources['water_cut_m3'] = active_sources.apply(
                lambda row: self.__get_water_cut_m3(row['wellid'], technology_well_data_map),
                axis=1)
            sources = pd.merge(sources, active_sources, on=['wellid', 'name', 'base_name', 'w_state', 'field_id'],
                               how='left')
            self.log.debug('Finished getting sources data')
            return sources
        except Exception as e:
            raise Exception('Error during process sources') from e

    def __calculate_gas_value(self, dob_data, technology_well):
        gas_factors = []
        for dob in dob_data:
            geological_characteristics = [x for x in
                                          self.db.geological_characteristics.get_oil_density_by_field_id_and_stratum_id(
                                              dob.field_id, dob.stratum_id)
                                          if
                                          x.density_oil_stratum is not None]
            if geological_characteristics:
                gas_factor = dob.gas_factor * geological_characteristics[0].density_oil_stratum
                if dob.rate_liquid_m3 is not None and technology_well.rate_liquid_m3 is not None:
                    rate_liquid_persent = dob.rate_liquid_m3 / technology_well.rate_liquid_m3
                    gas_factors.append({'gas_factor': gas_factor, 'rate_liquid_percent': rate_liquid_persent})
        weighted_gas_factors = [gas_factor['gas_factor'] * gas_factor['rate_liquid_percent'] for gas_factor in
                                gas_factors]
        if len(gas_factors) != 0:
            return sum(weighted_gas_factors) / sum(gas_factor['rate_liquid_percent'] for gas_factor in gas_factors)
        else:
            return self.config.default_gas_model_value

    def __find_gas(self, wellid, technology_well_data_map):
        if wellid in technology_well_data_map:
            if technology_well_data_map.get(wellid).gas_factor == 0.0:
                dob_data = self.db.technology_well_schema.get_technology_well_by_parts_by_wells_ids(wellid,
                                                                                                    self.config.search_date)
                if dob_data:
                    return self.__calculate_gas_value(dob_data, technology_well_data_map[wellid])
                else:
                    technology_well_data_map.get(wellid).gas_factor = self.config.default_gas_factor_value
                    return self.__find_gas_without_dob_data(wellid, technology_well_data_map)
            else:
                return self.__find_gas_without_dob_data(wellid, technology_well_data_map)

    def __find_gas_without_dob_data(self, wellid, technology_well_data_map):
        data = technology_well_data_map.get(wellid)
        geological_characteristics = [x for x in
                                      self.db.geological_characteristics.get_oil_density_by_field_id_and_stratum_id(
                                          data.field_id, data.stratum_id) if
                                      x.density_oil_stratum is not None]
        if geological_characteristics:
            return data.gas_factor * geological_characteristics[0].density_oil_stratum
        else:
            return self.config.default_gas_model_value

    @staticmethod
    def __get_rate_liquid_m3(wellid, technology_well_data_map):
        if wellid in technology_well_data_map:
            return technology_well_data_map.get(wellid).rate_liquid_m3

    @staticmethod
    def __get_water_cut_m3(wellid, technology_well_data_map):
        if wellid in technology_well_data_map:
            return technology_well_data_map.get(wellid).water_cut_m3
