def make_month_lims(M, leapyear = None):
    day_i = 1
    if M == 1 or M == 3 or M ==5 or M == 7 or M == 8 or M == 10 or M == 12:
        day_f = 31
    elif M == 2:
        if leapyear == True:
            day_f = 29
        else:
            day_f = 28
    else:
        day_f = 30
    return day_i, day_f

def create_annual(YEAR: int, PATH: str, filestart: int, fileend: int, **kwargs):
    
    """
    Given a path to a directory of NetCDF files with ROMS output data for a yearlong simulation,
    Return xarray Dataset with monthly averages for the full year.
    
    ds = create_annual(YEAR, PATH, filestart, fileend, isLeapYear = False)
    
    PARAMETERS
    ----------
    YEAR: int
        Target year
    PATH: str
        String with the path name to files
        Assumes individual file names follow the format ocean_his_####.nc
        e.g. '/Volumes/Baskind/ROMS/April 2024/apr2024_2005/'
    filestart: int
        ocean_his_####.nc file number to start from
        File number is zero-padded and converted to string to locate file
        e.g. 27 --> 'ocean_his_0027.nc' --> 'PATH/ocean_his_0027.nc'
    fileend: in
        ocean_his_####.nc file number to end with
        File number is zero-padded and converted to string to locate file
        e.g. 27 --> 'ocean_his_0027.nc' --> 'PATH/ocean_his_0027.nc'
        
    **kwargs
    --------
    isLeapyear: bool (optional)
        boolean (True or False) for whether YEAR is a leap year or not
        when not provided and year is 2008, 2012, 2016, 2020, or 2024, value set to True
        when not provided and year is not 2008, 2012, 2016, 2020, or 2024, value set to false
        when provided as wrong type, value set to False
        
    EXAMPLE
    -------
    ds = create_annual(2006, '/Volumes/Baskind/ROMS/April 2024/apr2024_2005/', 74, 100, isLeapYear = False)
    
    returns xarray dataset with dimensions     
        time: 12 (for each monthly average)
        tracer: 17
        boundary: 4
        nspc: 3
        s_rho: 10
        s_w: 11
        eta_rho: 550
        xi_rho: 500
        eta_u: 550
        xi_u: 499
        eta_v: 549
        xi_v: 500
    """
    
    import numpy as np
    import xarray as xr
    import pandas as pd
    import math
    from datetime import datetime, timedelta
    import time
    
    if 'isLeapYear' in kwargs:
        LEAPYEAR = kwargs.get('isLeapYear')
        if not isinstance(LEAPYEAR, bool):
            LEAPYEAR = False
            print("isLeapYear should be a boolean. Value set to False.")
    elif YEAR == 2008 or YEAR == 2012 or YEAR == 2016 or YEAR == 2020 or YEAR == 2024:
        LEAPYEAR = True
        print("No bool value set for isLeapYear. YEAR identified as leap year from limited set of leap years (2008, 2012, 2016, 2020, 2024).")
    else:
        LEAPYEAR = False
        print("isLeapYear set to default value (False).")
    
    # PATH = '/Volumes/Baskind/ROMS/April 2024/apr2024_2005/'
    filebase = 'ocean_his_'
    fileext = '.nc'
    
    # Open first dataset
    N = str(filestart).rjust(4, "0")
    FILE = PATH + filebase + N + fileext
    filename = filebase + N + fileext
    print(FILE)
    df = xr.open_mfdataset(FILE).mean('ocean_time')
    
    JAN = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    FEB = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    MAR = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    APR = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    MAY = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    JUN = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    JUL = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    AUG = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    SEP = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    OCT = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    NOV = xr.full_like(df, np.nan).assign_coords(time=np.nan)
    DEC = xr.full_like(df, np.nan).assign_coords(time=np.nan)

    nrange = np.arange(filestart,fileend,1)
    for m in nrange:
        N = str(m).rjust(4, "0")
        FILE = PATH + filebase + N + fileext
        filename = filebase + N + fileext
        df = xr.open_mfdataset(FILE)
        class color:
            PURPLE = '\033[95m'
            CYAN = '\033[96m'
            DARKCYAN = '\033[36m'
            BLUE = '\033[94m'
            GREEN = '\033[92m'
            YELLOW = '\033[93m'
            RED = '\033[91m'
            BOLD = '\033[1m'
            UNDERLINE = '\033[4m'
            END = '\033[0m'

        print(color.BOLD + '\nWorking on Month #' + str(df["ocean_time"].max().dt.month.max().values) + ' in ' + filename + '...\n' + color.END)
    
        # If working dataset only has values from a single month
        # i.e. max month equals min month of the dataset
        # And if the working dataset has only values from working year
        if df["ocean_time"].min().dt.month == df["ocean_time"].max().dt.month and df["ocean_time"].dt.year.mean() == YEAR:
            # Taking mean of month to reduce array to 1 value
            MONTH = df["ocean_time"].dt.month.mean()
            year = df["ocean_time"].dt.year.mean()
    
        # # Set day limits for the month
        # # e.g. October has days 1-31 but April has days 1-30
        # dayi, dayf = make_month_lims(MONTH)
        # ti = np.datetime64(datetime(2005,MONTH,dayi,0,0))
        # tf = np.datetime64(datetime(2005,MONTH,dayf,23,59))
        # # Time slice
        # ds = df.where(df["ocean_time"] >= ti).where(df["ocean_time"] < tf)
            ds = df
    
            # Get mean "filler" time
            t = ds['ocean_time'].mean()
            # Time mean
            ds = ds.mean('ocean_time')
            # Assign a time dimension with mean filler time
            ds = ds.assign_coords(time=t)
            if len(ds.keys()) > 200:
                ds = ds.drop_vars(["dstart"])
        
            del df
    
            # FOR EACH MONTH
            # Select the appropriate dataset for the month (e.g. JAN for January)
            # Concatenate working dataset with month dataset
            # Take the mean time for the month dataset as a filler time
            # Take time mean of month dataset and assign a time dim with filler time

            if year == YEAR:
                if MONTH == 1:                                             # JANUARY
                    JAN = xr.concat([ds,JAN], dim='time')
                    Tjan = JAN['time'].mean()
                    JAN = JAN.mean('time').assign_coords(time = Tjan)
                elif MONTH == 2:                                           # FEBRUARY
                    FEB = xr.concat([ds,FEB], dim='time')
                    T = FEB['time'].mean()
                    FEB = FEB.mean('time').assign_coords(time = T)
                elif MONTH == 3:                                           # MARCH
                    MAR = xr.concat([ds,MAR], dim='time')
                    T = MAR['time'].mean()
                    MAR = MAR.mean('time').assign_coords(time = T)
                elif MONTH == 4:                                           # APRIL
                    APR = xr.concat([ds,APR], dim='time')
                    T = APR['time'].mean()
                    APR = APR.mean('time').assign_coords(time = T)
                elif MONTH == 5:                                           # MAY
                    MAY = xr.concat([ds,MAY], dim='time')
                    T = MAY['time'].mean()
                    MAY = MAY.mean('time').assign_coords(time = T)
                elif MONTH == 6:                                           # JUNE
                    JUN = xr.concat([ds,JUN], dim='time')
                    T = JUN['time'].mean()
                    JUN = JUN.mean('time').assign_coords(time = T)
                elif MONTH == 7:                                           # JULY
                    JUL = xr.concat([ds,JUL], dim='time')
                    T = JUL['time'].mean()
                    JUL = JUL.mean('time').assign_coords(time = T)
                elif MONTH == 8:                                           # AUGUST
                    AUG = xr.concat([ds,AUG], dim='time')
                    T = AUG['time'].mean()
                    AUG = AUG.mean('time').assign_coords(time = T)
                elif MONTH == 9:                                           # SEPTEMBER
                    SEP = xr.concat([ds,SEP], dim='time')
                    T = SEP['time'].mean()
                    SEP = SEP.mean('time').assign_coords(time = T)
                elif MONTH == 10:                                          # OCTOBER
                    OCT = xr.concat([ds,OCT], dim='time')
                    T = OCT['time'].mean()
                    OCT = OCT.mean('time').assign_coords(time = T)
                elif MONTH == 11:                                          # NOVEMBER
                    NOV = xr.concat([ds,NOV], dim='time')
                    T = NOV['time'].mean()
                    NOV = NOV.mean('time').assign_coords(time = T)
                elif MONTH == 12:                                          # DECEMBER
                    DEC = xr.concat([ds,DEC], dim='time')
                    T = DEC['time'].mean()
                    DEC = DEC.mean('time').assign_coords(time = T)
                # del ds
        # =======================================================================================================================
        # =======================================================================================================================

        # If working dataset does NOT has values from a single month
        # i.e. max month does NOT equal min month of the dataset
        elif df["ocean_time"].dt.year.max() == YEAR:
            # Start with first (min) month ****************************************
            MONTH1 = df["ocean_time"].dt.month.min()
            year = df["ocean_time"].dt.year.min()

            # Set day limits for the first month
            # e.g. October has days 1-31 but April has days 1-30
            dayi1, dayf1 = make_month_lims(MONTH1, leapyear = LEAPYEAR)
            ti1 = np.datetime64(datetime(year,MONTH1,dayi1,0,0))
            tf1 = np.datetime64(datetime(year,MONTH1,dayf1,23,59))

            # Time slice
            ds1 = df.where(df["ocean_time"] >= ti1).where(df["ocean_time"] < tf1)
            # Get mean "filler" time
            t1 = ds1['ocean_time'].mean()
            if year != YEAR:
                t1 = np.nan

            # Time mean
            ds1 = ds1.mean('ocean_time')
            # Assign a time dimension with mean filler time
            ds1 = ds1.assign_coords(time=t1)
            if len(ds1.keys()) > 200:
                ds1 = ds1.drop_vars(["dstart"])

            # FOR EACH MONTH
            # Select the appropriate dataset for the month (e.g. JAN for January)
            # Concatenate working dataset with month dataset
            # Take the mean time for the month dataset as a filler time
            # Take time mean of month dataset and assign a time dim with filler time
            if year == YEAR:
                if MONTH1 == 1:                                             # JANUARY
                    JAN = xr.concat([ds1,JAN], dim='time')
                    Tjan = JAN['time'].mean()
                    JAN = JAN.mean('time').assign_coords(time = Tjan)
                elif MONTH1 == 2:                                           # FEBRUARY
                    FEB = xr.concat([ds1,FEB], dim='time')
                    T = FEB['time'].mean()
                    FEB = FEB.mean('time').assign_coords(time = T)
                elif MONTH1 == 3:                                           # MARCH
                    MAR = xr.concat([ds1,MAR], dim='time')
                    T = MAR['time'].mean()
                    MAR = MAR.mean('time').assign_coords(time = T)
                elif MONTH1 == 4:                                           # APRIL
                    APR = xr.concat([ds1,APR], dim='time')
                    T = APR['time'].mean()
                    APR = APR.mean('time').assign_coords(time = T)
                elif MONTH1 == 5:                                           # MAY
                    MAY = xr.concat([ds1,MAY], dim='time')
                    T = MAY['time'].mean()
                    MAY = MAY.mean('time').assign_coords(time = T)
                elif MONTH1 == 6:                                           # JUNE
                    JUN = xr.concat([ds1,JUN], dim='time')
                    T = JUN['time'].mean()
                    JUN = JUN.mean('time').assign_coords(time = T)
                elif MONTH1 == 7:                                           # JULY
                    JUL = xr.concat([ds1,JUL], dim='time')
                    T = JUL['time'].mean()
                    JUL = JUL.mean('time').assign_coords(time = T)
                elif MONTH1 == 8:                                           # AUGUST
                    AUG = xr.concat([ds1,AUG], dim='time')
                    T = AUG['time'].mean()
                    AUG = AUG.mean('time').assign_coords(time = T)
                elif MONTH1 == 9:                                           # SEPTEMBER
                    SEP = xr.concat([ds1,SEP], dim='time')
                    T = SEP['time'].mean()
                    SEP = SEP.mean('time').assign_coords(time = T)
                elif MONTH1 == 10:                                          # OCTOBER
                    OCT = xr.concat([ds1,OCT], dim='time')
                    T = OCT['time'].mean()
                    OCT = OCT.mean('time').assign_coords(time = T)
                elif MONTH1 == 11:                                          # NOVEMBER
                    NOV = xr.concat([ds1,NOV], dim='time')
                    T = NOV['time'].mean()
                    NOV = NOV.mean('time').assign_coords(time = T)
                elif MONTH1 == 12:                                          # DECEMBER
                    DEC = xr.concat([ds1,DEC], dim='time')
                    T = DEC['time'].mean()
                    DEC = DEC.mean('time').assign_coords(time = T)

            # Go to next (max) month ***********************************************
            MONTH2 = df["ocean_time"].dt.month.max()
            year = df["ocean_time"].dt.year.max()

            # Set day limits for the first month
            # e.g. October has days 1-31 but April has days 1-30
            dayi2, dayf2 = make_month_lims(MONTH2, leapyear = LEAPYEAR)
            ti2 = np.datetime64(datetime(year,MONTH2,dayi2,0,0))
            tf2 = np.datetime64(datetime(year,MONTH2,dayf2,23,59))

            # Time slice
            ds2 = df.where(df["ocean_time"] >= ti2).where(df["ocean_time"] < tf2)
            # Get mean "filler" time
            t2 = ds2['ocean_time'].mean()

            # Time mean
            ds2 = ds2.mean('ocean_time')
            # Assign a time dimension with mean filler time
            ds2 = ds2.assign_coords(time=t2)
            if len(ds2.keys()) > 200:
                ds2 = ds2.drop_vars(["dstart"])

            # FOR EACH MONTH
            # Select the appropriate dataset for the month (e.g. JAN for January)
            # Concatenate working dataset with month dataset
            # Take the mean time for the month dataset as a filler time
            # Take time mean of month dataset and assign a time dim with filler time
            if year == YEAR:
                if MONTH2 == 1:                                             # JANUARY
                    JAN = xr.concat([ds2,JAN], dim='time')
                    Tjan = JAN['time'].mean()
                    JAN = JAN.mean('time').assign_coords(time = Tjan)
                elif MONTH2 == 2:                                           # FEBRUARY
                    FEB = xr.concat([ds2,FEB], dim='time')
                    T = FEB['time'].mean()
                    FEB = FEB.mean('time').assign_coords(time = T)
                elif MONTH2 == 3:                                           # MARCH
                    MAR = xr.concat([ds2,MAR], dim='time')
                    T = MAR['time'].mean()
                    MAR = MAR.mean('time').assign_coords(time = T)
                elif MONTH2 == 4:                                           # ARPIL
                    APR = xr.concat([ds2,APR], dim='time')
                    T = APR['time'].mean()
                    APR = APR.mean('time').assign_coords(time = T)
                elif MONTH2 == 5:                                           # MAY
                    MAY = xr.concat([ds2,MAY], dim='time')
                    T = MAY['time'].mean()
                    MAY = MAY.mean('time').assign_coords(time = T)
                elif MONTH2 == 6:                                           # JUNE
                    JUN = xr.concat([ds2,JUN], dim='time')
                    T = JUN['time'].mean()
                    JUN = JUN.mean('time').assign_coords(time = T)
                elif MONTH2 == 7:                                           # JULY
                    JUL = xr.concat([ds2,JUL], dim='time')
                    T = JUL['time'].mean()
                    JUL = JUL.mean('time').assign_coords(time = T)
                elif MONTH2 == 8:                                           # AUGUST
                    AUG = xr.concat([ds2,AUG], dim='time')
                    T = AUG['time'].mean()
                    AUG = AUG.mean('time').assign_coords(time = T)
                elif MONTH2 == 9:                                           # SEPTEMBER
                    SEP = xr.concat([ds2,SEP], dim='time')
                    T = SEP['time'].mean()
                    SEP = SEP.mean('time').assign_coords(time = T)
                elif MONTH2 == 10:                                          # OCTOBER
                    OCT = xr.concat([ds2,OCT], dim='time')
                    T = OCT['time'].mean()
                    OCT = OCT.mean('time').assign_coords(time = T)
                elif MONTH2 == 11:                                          # NOVEMBER
                    NOV = xr.concat([ds2,NOV], dim='time')
                    T = NOV['time'].mean()
                    NOV = NOV.mean('time').assign_coords(time = T)
                elif MONTH2 == 12:                                          # DECEMBER
                    DEC = xr.concat([ds2,DEC], dim='time')
                    T = DEC['time'].mean()
                    DEC = DEC.mean('time').assign_coords(time = T)
                # del ds1, ds2
                
                
    ANN = xr.concat([JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC], dim='time')

    t = np.zeros(12)
    for i in range(12):
        MONTH = str(i + 1).rjust(2, "0")
        datestr = str(YEAR) + '-' + MONTH + '-' + '15' + 'T12:00:00.0000000Z'
        dt = datetime(YEAR, i + 1, 15, 12, 34, 56)
        t[i] = np.datetime64(datestr)
    t = t.astype("datetime64[ns]")
    ANN = ANN.assign_coords(time = t)
    return ANN