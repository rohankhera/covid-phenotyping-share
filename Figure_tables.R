#The following code sets were used to generate different aspects of the study

## Cumulative Plot - Figure 1

library(ggthemes)
pcrs <- tabledata %>% filter(lab_dx==1) %>% select(measurement_datetime) %>%  rename(date=measurement_datetime) %>% mutate(category="PCR")
primaries <- tabledata %>% filter(pri_covid_dx_yes==1) %>% select(primary_covid_dx_start_datetime) %>% rename(date = primary_covid_dx_start_datetime) %>% mutate(category="Principal dx")

anies <- tabledata %>% filter(any_covid_dx_yes==1) %>% select(any_covid_dx_start_datetime) %>% rename(date=any_covid_dx_start_datetime) %>% mutate(category="Any dx")

overalls <- tabledata %>% filter(overall) %>% select(admitdate) %>% rename(date=admitdate)%>% mutate(category="Overall")


cumdata <- rbind(pcrs,primaries,anies,overalls)
cumdata$date <- as.Date(cumdata$date)

cumdata$category <- factor(cumdata$category, levels = c("Overall","Any dx", "Principal dx", "PCR"))

table(cumdata %>% filter(is.na(date)) %>% select(category))

ggplot(cumdata %>% group_by(category) %>% arrange(date) %>% mutate(rn=row_number())) +geom_step(aes(x=date, y=rn, color=category)) + ylab("Cumulative Case Counts, N") +xlab("Date")  +theme_stata() + scale_colour_stata()+theme(legend.title=element_blank())



#Figure 2 and 4 have Venn or Euler Diagrams - representative code is included below

library("eulerr")

plot(euler(c("Any Diagnosis" = n1, "Principal Diagnosis" =0, "PCR/Antigen+" = n2, "Any Diagnosis&Principal Diagnosis"=n3, "Any Diagnosis&PCR/Antigen+"=n4, "Any Diagnosis&PCR/Antigen+&Principal Diagnosis"=n5),by=, shape="ellipse"), quantities = list(type = c("counts", "percent")), edges=list(lty=5,lwd=0.7))

#n1, n2, n3, n4 represent mutually exclusive areas of the venn or areas of overlap for the combinations


#Table1

library(tableone)
tabledata <- data2 %>% select(person_id, pri_covid_dx_yes, primary_covid_dx_start_datetime, any_covid_dx_start_datetime,measurement_datetime ,any_covid_dx_yes, lab_dx, gender_source_value,race_source_value, ethnicity_source_value, date_of_birth)
tabledata <- tabledata %>% mutate(admitdate0 = ifelse(primary_covid_dx_start_datetime=="",any_covid_dx_start_datetime,primary_covid_dx_start_datetime)) %>% mutate(admitdate = ifelse(admitdate0=="",measurement_datetime,admitdate0))
tabledata <- tabledata %>% mutate(age = time_length(difftime(as.Date(admitdate), as.Date(date_of_birth)),"years"))
tabledata[is.na(tabledata)] <- 0

tabledata$race_source_value <- factor(tabledata$race_source_value)
tabledata$race_source_value
levels(tabledata$race_source_value) <- c("Unknown","American Indian or Alaska Native","Asian","Black or African American","Native Hawaiian or Other Pacific Islander", "Native Hawaiian or Other Pacific Islander","Other Race","Native Hawaiian or Other Pacific Islander","Other Race", "Unknown", "Unknown", "White")


tabledata$ethnicity_source_value <- factor(tabledata$ethnicity_source_value)
levels(tabledata$ethnicity_source_value) <- c("Unknown","Hispanic or Latino","Not Hispanic or Latino","Unknown","Unknown")

vars <- c("age", "gender_source_value", "race_source_value", "ethnicity_source_value")

tableOne <- CreateTableOne(vars = vars, strata = "pri_covid_dx_yes", data = tabledata, addOverall = TRUE)
tableOne <- CreateTableOne(vars = vars, strata = "any_covid_dx_yes", data = tabledata, addOverall = TRUE)
tableOne <- CreateTableOne(vars = vars, strata = "lab_dx", data = tabledata, addOverall = TRUE)

tabledata <- tabledata %>% mutate(overall = (pri_covid_dx_yes + any_covid_dx_yes + lab_dx) > 0)

tableOne <- CreateTableOne(vars = vars, strata = "overall", data = tabledata, addOverall = TRUE)

print(tableOne, nonnormal = c("los_days"), quote = TRUE, noSpaces = TRUE)
