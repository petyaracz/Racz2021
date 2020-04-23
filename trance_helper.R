# pulls data in from the DPLACE github repository, tidies it up, saves it
# author: pracz
# date: 24/4/20

setwd('~/Github/Racz2021/')
library(tidyverse)
library(rjson)

# --- pull --- #

variables = readr::read_csv("https://raw.githubusercontent.com/D-PLACE/dplace-data/master/datasets/EA/variables.csv")

codes = readr::read_csv("https://raw.githubusercontent.com/D-PLACE/dplace-data/master/datasets/EA/codes.csv")

codes = codes %>% # we need some changes in codes
  dplyr::rename(
    var_description = description,
    var_name = name
  ) %>% 
  dplyr::mutate(
    num_id = str_replace(var_id, 'EA', '') %>% as.double() %>% as.character()
  )

dat = readr::read_csv("https://raw.githubusercontent.com/D-PLACE/dplace-data/master/datasets/EA/data.csv")

languages = readr::read_csv("https://raw.githubusercontent.com/D-PLACE/dplace-data/master/csv/glottolog.csv") %>% 
  rename(family = family_name, glottocode = id) %>% 
  select(family, glottocode)

sccs = readr::read_csv("https://raw.githubusercontent.com/D-PLACE/dplace-data/master/datasets/SCCS/societies.csv") %>% 
  dplyr::select(pref_name_for_society,glottocode,HRAF_name_ID) %>% 
  rename(society = pref_name_for_society)

# for societies, we don't keep everything and rename some things
societies = readr::read_csv("https://raw.githubusercontent.com/D-PLACE/dplace-data/master/datasets/EA/societies.csv") %>% 
  dplyr::select(id, glottocode, pref_name_for_society, Lat, Long) %>% 
  dplyr::rename(
    lat = Lat, 
    lon = Long, 
    soc_id = id, 
    society = pref_name_for_society
  )

# locations is a json. the json converter doesn't quite cope with it so we need some tricks
locations = RCurl::getURL("https://raw.githubusercontent.com/D-PLACE/dplace-data/master/geo/societies_tdwg.json") %>%
  fromJSON # makes it into a list of lists
soc_id = names(locations)
locations = locations %>% 
  purrr::map( ~ vctrs::vec_rbind(.)) %>% # turn the lists in the list into tibbles
  purrr::map( ~ dplyr::mutate_all(., as.character)) %>% # change all columns in the tibbles in the list to char
  dplyr::bind_rows() # bind tibbles in list into big tibble (this won't work w/o prev line for reasons)
locations$soc_id = soc_id

# --- shape --- #

# joining data
names(variables) = paste0('var_', names(variables)) # keep names of variables separate
locations = locations %>% # we need to fix locations now
  dplyr::select(soc_id, name) %>% 
  dplyr::rename(region = name)

dat = dat %>% 
  dplyr::mutate(
  num_id = str_replace(var_id, 'EA', '') %>% as.double() %>% as.character()
)

# --- filter --- #

keep_vars = paste('EA', c("034","012","008","015","023","043","027","031","030","032","033","202","042","066","068","070","072","074","078","113","112"), sep='')

trance_dict = codes %>% 
  filter(var_id %in% keep_vars) %>% 
  mutate(
    recoded_value =
      case_when(
        var_id == 'EA008' & code %in% c(1:2) ~ '1',
        var_id == 'EA008' & code %in% c(3:8) ~ '0',
        var_id == 'EA012' & code %in% c(4,8,10,12) ~ 'husband',
        var_id == 'EA012' & code %in% c(1,9,3,5,11) ~ 'wife',
        var_id == 'EA012' & code %in% c(2,6,7) ~ 'other',
        var_id == 'EA015' & code %in% c(1,2) ~ "endogamous",
        var_id == 'EA015' & code %in% c(3) ~ "agamous",
        var_id == 'EA015' & code %in% c(4,5,6) ~ "exogamous",
        var_id == 'EA023' & code %in% c(7,8) ~ '1',
        var_id == 'EA023' & code %in% c(11,12) ~ '2',
        var_id == 'EA023' & code %in% c(1,2,3,4,5,6,9,13) ~ '3',
        var_id == 'EA023' & code %in% c(10) ~ '4',
        var_id == 'EA043' & code %in% c(1) ~ "patrilineal",
        var_id == 'EA043' & code %in% c(2:7) ~ "other",
        var_id == 'EA027' & code %in% c(4) ~ '1',
        var_id == 'EA027' & code %in% c(3) ~ '2',
        var_id == 'EA027' & code %in% c(5,8) ~ '3',
        var_id == 'EA027' & code %in% c(1,6) ~ '4',
        var_id == 'EA027' & code %in% c(7,2) ~ '5',
        var_id == 'EA042' & code %in% c(7) ~ "int_agr",
        var_id == 'EA042' & code %in% c(5,6,9) ~ "ext_agr",
        var_id == 'EA042' & code %in% c(4) ~ "pastoralism",
        var_id == 'EA042' & code %in% c(1,2,3) ~ "foraging",
        var_id == 'EA072' & code %in% c(1) ~ 'patrilineal',
        var_id == 'EA072' & code %in% c(2) ~ 'matrilineal',
        var_id == 'EA072' & code %in% c(3,4,5,6,7) ~ 'other',
        var_id == 'EA072' & code %in% c(9) ~ 'absent',
        var_id == 'EA074' & code %in% c(6,7) ~ 'patrilineal',
        var_id == 'EA074' & code %in% c(2,3) ~ 'matrilineal',
        var_id == 'EA074' & code %in% c(4,5) ~ 'other',
        var_id == 'EA074' & code %in% c(1) ~ 'absent',
        var_id == 'EA113' & code == 1 ~ 'T',
        var_id == 'EA113' & code == 2 ~ 'F',
        var_id == 'EA112' & code == 8 ~ '1', # out: no trance, no pos
        var_id == 'EA112' & code %in% c(1:2) ~ '2', # out: trance xor pos
        var_id == 'EA112' & code == 5 ~ '3', # out: trance and pos, independent
        var_id == 'EA112' & code %in% c(4,6,7) ~ '4', # out: pos causes trance, maybe other trance, maybe other pos
        var_id == 'EA112' & code == 3 ~ '5', # out: pos causes trance, nothing else causes t, pos causes nothing else
        TRUE ~ as.character(code)
      )
  )



# --- wrangling --- #

variables_dict = variables %>% 
  select(var_id, var_title, var_type) %>% 
  right_join(trance_dict, by = "var_id") %>% 
  mutate(var_title2 = var_title %>% str_replace_all(' ', '_') %>% 
           str_replace_all('[)(:]', '')
  )

observations_dict = societies %>% 
  left_join(languages, by = "glottocode") %>% 
  left_join(locations, by = "soc_id")

min_dat = select(dat, var_id, num_id, soc_id, code)

# --- combining --- #

max_dat = left_join(min_dat, observations_dict, by = "soc_id")
variables_dict1 = select(variables_dict, var_id, var_title, var_type, var_title2)
nrow(max_dat)
max_dat = right_join(max_dat, variables_dict) # var dict is a subset of the variables in max dat rn
# count(distinct(max_dat, society))
# count(distinct(max_dat, var_title))

# --- write --- #

write_csv(codes, 'data/ea/t_codes.csv')
write_csv(languages, 'data/ea/t_languages.csv')
write_csv(sccs, 'data/ea/t_sccs_id.csv')
write_csv(societies, 'data/ea/t_societies.csv')
write_csv(variables, 'data/ea/t_variables.csv')
write_csv(dat, 'data/ea/t_ea.csv')
write_csv(trance_dict, 'data/ea/t_dict.csv')

write_csv(variables_dict, 'data/t_var_dict.csv')
write_csv(observations_dict, 'data/t_obs_dict.csv')
write_csv(min_dat, 'data/t_dat.csv')
write_csv(max_dat, 'data/t_dat_full.csv')
