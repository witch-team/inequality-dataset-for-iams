# Process Distributional Data created for the NAVIGATE project
# Author: Johannes Emmerling, data collection: Shouro Dasgupta, Simon Feindt, Daniele Malerba, Carolina Grottera
require(data.table)
require(countrycode)
require(stringr)
require(tidyverse)
require(openxlsx)

folder = "settlement_gender"

survey_inequality_filelist <- list.files(path = folder, pattern = "Inequality Input Data Template.*.csv$")
survey_inequality_filelist <- survey_inequality_filelist[!str_detect(survey_inequality_filelist, "GENDER") & !str_detect(survey_inequality_filelist, "EMPTY")]
#.file <- survey_inequality_filelist[1]
allvars <- c("expcat_input", "incomecat", "savings_rate", "wealth_share", "educat", "inequality_index", "household_size", "expenditure_decile", "income_decile")

for (.file in survey_inequality_filelist) {
  data <- fread(file.path(folder, .file), header = T)
  #specific command for EU
  if(.file=="Inequality Input Data Template CMCC_EU.csv") data <- data %>% filter(REGION!="FRA") #%>% filter(!str_detect(VARIABLE, "Emissions"))
  
  data_output <- data %>% select(-MODEL, -SCENARIO, -UNIT, -str_subset(names(data), "V[0-9]")) %>% rename(iso3=REGION) %>% mutate(dist=ifelse(str_detect(VARIABLE, "Inequality Index"), "0", str_extract(VARIABLE, "D[0-9].*$")))
  data_output <- data_output %>% mutate(var=case_when(str_detect(VARIABLE, "Expenditure Share") ~ "expcat_input", str_detect(VARIABLE, "Income Share") ~ "incomecat", str_detect(VARIABLE, "Savings Rate") ~ "savings_rate", str_detect(VARIABLE, "Wealth Share") ~ "wealth_share", str_detect(VARIABLE, "Education") ~ "educat", str_detect(VARIABLE, "Wage Premium") ~ "wage_premium", str_detect(VARIABLE, "Expenditure Decile") ~ "expenditure_decile",  str_detect(VARIABLE, "Income Decile") ~ "income_decile", str_detect(VARIABLE, "Inequality Index") ~ "inequality_index", str_detect(VARIABLE, "Household Size") ~ "household_size", str_detect(VARIABLE, "Emissions per capita") ~ "emissions_per_capita"))
  data_output <- data_output %>% filter(!str_detect(VARIABLE, "Meat")) #for now don't separate out meat consumption
  data_output <- data_output %>% mutate(element=case_when(str_detect(VARIABLE, "Housing") ~ "energy_housing", str_detect(VARIABLE, "Transportation") ~ "energy_transportation", str_detect(VARIABLE, "Food") ~ "food", str_detect(VARIABLE, "Expenditure Share|Other") ~ "other", str_detect(VARIABLE, "Labour") ~ "labour", str_detect(VARIABLE, "Capital") ~ "capital", str_detect(VARIABLE, "Transfers") ~ "transfers", str_detect(VARIABLE, "Income Share|Other") ~ "other", str_detect(VARIABLE, "Under 15") ~ "Under 15", str_detect(VARIABLE, "No Education") ~ "No education", str_detect(VARIABLE, "Primary Education") ~ "Primary Education", str_detect(VARIABLE, "Secondary Education") ~ "Secondary Education", str_detect(VARIABLE, "Tertiary Education") ~ "Tertiary Education", str_detect(VARIABLE, "Gini") ~ "gini", str_detect(VARIABLE, "Absolute Poverty") ~ "absolute_poverty"))
  
  data_output <- data_output %>% select(-VARIABLE) %>% pivot_longer(cols = setdiff(names(data_output), c("iso3", "VARIABLE", "dist", "var", "element")), names_to = "year") %>% as.data.frame()
  data_output <- data_output %>% select(year, iso3, var, element, dist, value)
  print(.file); #print(unique(data_output$var))
  #print(str(data_output))
  print("Variables missing:"); print(allvars[!(allvars %in% unique(data_output$var))])
  if(.file==survey_inequality_filelist[1]){
    data_output_allcountries <- data_output
    data_input_format <- data[,1:6]
  }else{
    data_output_allcountries <- rbind(data_output_allcountries, data_output)
    data_input_format <- rbind(data_input_format, data[,1:6], fill=T)
  } 
}

# STORE DATA combined CSV file
fwrite(data_output_allcountries, file = file.path(folder, "deciles_data.csv"))

#Show list of countries and variables
ggplot(data_input_format %>% filter(str_detect(VARIABLE, "D10")) %>% group_by(REGION, VARIABLE) %>% summarize(avail=length(UNIT)) %>% mutate(VARIABLE=gsub("\\|D10", "", VARIABLE)), aes(REGION,VARIABLE, fill = avail)) + geom_tile() + theme_minimal() + theme(axis.text.x = element_text(angle=90, vjust = 0.5)) + guides(fill="none") 
ggsave(path = folder, "Countries and Variables.png", width = 10, height=8)
#show expenditure shares
ggplot(data_output_allcountries %>% filter(element!="food" & var=="expcat_input" & ((iso3=="ZAF" & year=="2017") | iso3!="ZAF")) %>% mutate(decile=(as.numeric(gsub("D", "", dist))))) + geom_line(aes(decile, value, color=element)) + facet_wrap(. ~ iso3, scales = "free_x") + theme_minimal() + theme(legend.position = "bottom") + labs(x="", y="") 
ggplot(data_output_allcountries %>% filter(element!="food" & var=="expcat_input" & ((iso3=="ZAF" & year=="2017") | iso3!="ZAF")) %>% mutate(decile=(as.numeric(gsub("D", "", dist))))) + geom_line(aes(decile, value, color=iso3)) + facet_wrap(. ~ element, scales = "free_x") + theme_minimal() + theme(legend.position = "bottom") + labs(x="Decile", y="Expenditure share [%]") + scale_x_continuous(breaks=seq(1,10)) 
ggsave(path = folder, "Energy Expenditure Shares.png", width = 10, height=8)
#Show deciles
ggplot(data_output_allcountries %>% filter(var=="income_decile" & ((iso3=="ZAF" & year=="2017") | iso3!="ZAF")) %>% mutate(decile=(as.numeric(gsub("D", "", dist))))) + geom_line(aes(decile, value)) + facet_wrap(. ~ iso3, scales = "free_x") + theme_minimal() + theme(legend.position = "bottom") + labs(x="", y="") 
ggplot(data_output_allcountries %>% filter(var=="expenditure_decile" & ((iso3=="ZAF" & year=="2017") | iso3!="ZAF")) %>% mutate(decile=(as.numeric(gsub("D", "", dist))))) + geom_line(aes(decile, value)) + facet_wrap(. ~ iso3, scales = "free_x") + theme_minimal() + theme(legend.position = "bottom") + labs(x="", y="") 
ggsave(path = folder, "Expenditure deciles.png", width = 10, height=8)





#India first trial
data_test <- openxlsx::read.xlsx(xlsxFile = file.path(folder, "Inequality Input Data Template CMCC Settlement_Gender IND.xlsx"), sheet = 1, cols = 1:12)
data_test <- fill(data_test, c(MODEL, SCENARIO, ISO3, VARIABLE, UNIT, Definition, number.of.variables))
data_test <- data_test %>% mutate(value=as.numeric(X12))
unique(data_test$VARIABLE)
ggplot(data_test %>% filter(VARIABLE=="Expenditure Share|Energy|Housing|Female|Urban|Dx")) + geom_line(aes(decile, value, color=gender, linetype=`intra-hh.allocation`)) + facet_wrap(. ~ settlement)
ggsave(path = folder, "Gender_settlement_housing.png", width = 10, height=6)
ggplot(data_test %>% filter(VARIABLE=="Expenditure Share|Energy|Transportation|Female|Urban|Dx")) + geom_line(aes(decile, value, color=gender, linetype=`intra-hh.allocation`)) + facet_wrap(. ~ settlement)
ggsave(path = folder, "Gender_settlement_transportation.png", width = 10, height=6)

ggplot(data_test %>% filter(VARIABLE=="Income per capita|Female|Urban|Dx")) + geom_bar(aes(decile, value*0.012, fill=gender), stat="identity") + facet_grid(settlement ~ gender) + labs(y="Income per capita [USD[2020]]") + scale_y_continuous(labels = scales::label_dollar())
ggsave(path = folder, "Gender_settlement_income.png", width = 10, height=6)
