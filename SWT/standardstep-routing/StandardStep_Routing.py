# name=StandardStep_Routing
# description=Input: hydrograph
# description=Output: standard step routing coefficients used by RiverWare
# description=Change TEST to the name of the reservoir making bankfull releases
# description=Releases are made from only one reservoir at a time and routed down through the Arkansas River Basin to Van Buren gage using the HEC-1 VANT forecast model. This model has been changed to only include the routing of flow through the Arkansas River system.
# description=The hydrographs are then selected and run using this script to create the standard step routing coefficients for use in the TAPER RiverWare model. This process needs to be completed for every reservoir in the TAPER model.
# displayinmenu=true
# displaytouser=true
# displayinselector=true
# from hec.script import *
# from hec.dssgui import ListSelection
# from hec.heclib.dss import *
# from hec.heclib.util import HecDouble

from hecdss import HecDss, ArrayContainer


__version__ = "2.0.0"


# Reads in the selected data on the MainWindow of HEC-DSSVue
mainWindow = ListSelection.getMainWindow()
dssFileName = mainWindow.getDSSFilename()
dssFile = HecDss.open(dssFileName)
dssPathNames = mainWindow.getSelectedPaths()
dssTaper = HecDss.open(
    "V:/Riverware/TAPER/StandardStep_Routing/TAPER_StandardStep.dss"
)  # output file location

TEST = "/test/"  # change to name of reservoir making releases
# dssFileName = HecDss.open("C:/forecast/work/VANT.DSS")
# flow = dssFileName.read("//RHASK/FLOW-LOC CUM/01JAN2015/1HOUR/CALC/")

# Calculates the standard step routing coefficients as the percent of the total release  that has passed for 6-hour increments
for i in range(len(dssPathNames)):
    flow = dssFile.read(dssPathNames[i])
    total = flow.sum()  # sums flow

    if total > 0:
        standard_step = flow.divide(
            total
        )  # determine percent of flow passing at each timestep
        # standard_step_6hr = standard_step.transformTimeSeries("6HOUR", "0M", "ACC") #creates a period average, need instantanous or a true accumulation up until that 6 hour pt
        standard_step_6hr = (
            standard_step.accumulation()
            .transformTimeSeries("6HOUR", "0M", "INT")
            .successiveDifferences()
        )
        standard_step_6hr = standard_step_6hr.roundOff(6, -4)  # sets precision
        error = 1 - standard_step_6hr.sum()
        print(error)
        maxVal = standard_step_6hr.max()
        maxDateTime = standard_step_6hr.maxDate()
        newmaxVal = maxVal + error
        print(maxVal)
        print(newmaxVal)
        standard_step_6hr = standard_step_6hr.replaceSpecificValues(
            HecDouble(maxVal), HecDouble(newmaxVal)
        )
        # standard step values must add up to 1.0. The error created through setting the precision is small, but is add to the maximum value to make the coefficients add up to 1.0.

        standard_step = standard_step.getData()
        standard_step_6hr = standard_step_6hr.getData()
        # name the new DssVue paths for the calculated values
        standard_step.fullName = standard_step.fullName.replace("CALC", "REACHSTEP")
        standard_step_6hr.fullName = standard_step_6hr.fullName.replace(
            "CALC", "REACHSTEP"
        )
        standard_step.fullName = standard_step.fullName.replace("//", TEST)
        standard_step_6hr.fullName = standard_step_6hr.fullName.replace("//", TEST)
        standard_step.fileName = "V:/Riverware/TAPER/StandardStep_Routing/TAPER_StandardStep.dss"  # output file
        standard_step_6hr.fileName = (
            "V:/Riverware/TAPER/StandardStep_Routing/TAPER_StandardStep.dss"
        )
        dssTaper.put(standard_step)
        dssTaper.put(standard_step_6hr)


dssFile.done()
