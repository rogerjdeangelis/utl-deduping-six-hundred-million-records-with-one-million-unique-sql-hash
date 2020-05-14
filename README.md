# utl-deduping-six-hundred-million-records-with-one-million-unique-sql-hash
Deduping-six-hundred-million-records-with-one-million-unique-sql-hash
    Deduping-six-hundred-million-records-with-one-million-unique-sql-hash;                                                              
                                                                                                                                        
       Four Solutions                                   Seconds                                                                         
                                                        =======                                                                         
           a. sql                                        332                                                                            
                                                                                                                                        
           b. hash grouped X1 sasfile data in memory      36  (good locality of reference sasfile load not counted)                     
              Bartosz Jablonski yabwon@gmail.com                                                                                        
                                                                                                                                        
           c. hash random X1  sasfile data in memory     116  (random scatterd data)                                                    
              Bartosz Jablonski yabwon@gmail.com                                                                                        
                                                                                                                                        
           d. 16 parallel sql tasks                       38  (16 parallel SQL tasks - used a datastep to put 16 pieces bask together   
                                                               could use union corr - it automatically does a first dot)                
    github                                                                                                                              
    https://tinyurl.com/yc8kp5dd                                                                                                        
    https://github.com/rogerjdeangelis/utl-deduping-six-hundred-million-records-with-one-million-unique-sql-hash                        
                                                                                                                                        
    600,000,000 is a small dataset if you have only one columm                                                                          
                                                                                                                                        
    If the data is grouped but not sorted you could run a first dot with the nontsorted option.                                         
    This could quickly reduce the size of the input dataset for SQL processing.                                                         
                                                                                                                                        
    see                                                                                                                                 
    https://github.com/rogerjdeangelis?tab=repositories&q=systask+&type=&language=                                                      
                                                                                                                                        
    SAS Forum                                                                                                                           
    https://tinyurl.com/ybjfldc5                                                                                                        
    https://communities.sas.com/t5/SAS-Programming/How-to-get-duplicate-records-as-output-data-set-using-proc-sql/m-p/647386            
                                                                                                                                        
    *_                   _                                                                                                              
    (_)_ __  _ __  _   _| |_                                                                                                            
    | | '_ \| '_ \| | | | __|                                                                                                           
    | | | | | |_) | |_| | |_                                                                                                            
    |_|_| |_| .__/ \__,_|\__|                                                                                                           
            |_|                                                                                                                         
    ;                                                                                                                                   
                                                                                                                                        
    data have;                                                                                                                          
      retain rnd;                                                                                                                       
      call streaminit(4321);                                                                                                            
      do unique=1 to 1e6;                                                                                                               
         x1=put(int(1000000000*rand('uniform')),z9.);                                                                                   
         do dups=1 to int(1210*rand('uniform'));                                                                                        
           rnd=rand('uniform');                                                                                                         
           output;                                                                                                                      
         end;                                                                                                                           
      end;                                                                                                                              
      drop unique dups;                                                                                                                 
    run;quit;                                                                                                                           
                                                                                                                                        
    WORK.HAVE total obs=604,461,078                                                                                                     
    I use rnd later to scramble the grouped data                                                                                        
                                                                                                                                        
        RND         X1                                                                                                                  
                                                                                                                                        
      0.92960    790842114  * smallest group 1 record, largest group 1,200                                                              
      0.20874    790842114                                                                                                              
      0.21611    790842114                                                                                                              
      0.69309    790842114                                                                                                              
      0.45677    790842114                                                                                                              
      0.27118    790842114                                                                                                              
      0.46014    790842114                                                                                                              
      0.37119    790842114                                                                                                              
      0.87254    790842114                                                                                                              
      0.21248    790842114                                                                                                              
     ...                                                                                                                                
                                                                                                                                        
                                                                                                                                        
    NOTE: The data set WORK.HAVE has 604461078 observations and 2 variables.                                                            
    NOTE: DATA statement used (Total process time):                                                                                     
          real time           28.92 seconds                                                                                             
          cpu time            28.87 seconds                                                                                             
                                                                                                                                        
    proc sort data=have out=havsrt(drop=rnd) noequals sortsize=64gb;                                                                    
    by rnd;                                                                                                                             
    run;quit;                                                                                                                           
                                                                                                                                        
    NOTE: The data set WORK.HAVSRT has 604461078 observations and 1 variables.                                                          
    NOTE: PROCEDURE SORT used (Total process time):                                                                                     
          real time           6:46.67                                                                                                   
          cpu time            9:51.50                                                                                                   
                                                                                                                                        
                                                                                                                                        
    Work.HAVSRT total obs=604,461,078                                                                                                   
                                                                                                                                        
         Obs       X1                                                                                                                   
                                                                                                                                        
           1    783360389   * random order worst case                                                                                   
           2    649598740   * no locality of reference here                                                                             
           3    506265692   * sql will be faster if you have grouped X1                                                                 
           4    169903883                                                                                                               
           5    451750192                                                                                                               
           6    116310264                                                                                                               
           7    833764465                                                                                                               
           8    139512013                                                                                                               
           9    563884642                                                                                                               
          10    010058501                                                                                                               
          11    412407778                                                                                                               
          12    626357393                                                                                                               
                                                                                                                                        
    *            _               _                                                                                                      
      ___  _   _| |_ _ __  _   _| |_                                                                                                    
     / _ \| | | | __| '_ \| | | | __|                                                                                                   
    | (_) | |_| | |_| |_) | |_| | |_                                                                                                    
     \___/ \__,_|\__| .__/ \__,_|\__|                                                                                                   
                    |_|                                                                                                                 
    ;                                                                                                                                   
                                                                                                                                        
    WORK.WANT total obs=998,674                                                                                                         
                                                                                                                                        
       Obs       X1                                                                                                                     
                                                                                                                                        
         1    000000688                                                                                                                 
         2    000001990                                                                                                                 
         3    000002043                                                                                                                 
         4    000002774                                                                                                                 
         5    000005980                                                                                                                 
         6    000007028                                                                                                                 
         7    000008179                                                                                                                 
         8    000008749                                                                                                                 
         9    000009438                                                                                                                 
        10    000009718                                                                                                                 
        11    000010349                                                                                                                 
        12    000010876                                                                                                                 
        13    000010942                                                                                                                 
        14    000012855                                                                                                                 
        15    000013465                                                                                                                 
       ...                                                                                                                              
    *                                                                                                                                   
     _ __  _ __ ___   ___ ___  ___ ___                                                                                                  
    | '_ \| '__/ _ \ / __/ _ \/ __/ __|                                                                                                 
    | |_) | | | (_) | (_|  __/\__ \__ \                                                                                                 
    | .__/|_|  \___/ \___\___||___/___/                                                                                                 
    |_|            _             _        _            _                _                                                               
       _ _     ___(_)_ __   __ _| | ___  | |_ __ _ ___| | __  ___  __ _| |                                                              
     / _` |   / __| | '_ \ / _` | |/ _ \ | __/ _` / __| |/ / / __|/ _` | |                                                              
    | (_| |_  \__ \ | | | | (_| | |  __/ | || (_| \__ \   <  \__ \ (_| | |                                                              
     \__,_(_) |___/_|_| |_|\__, |_|\___|  \__\__,_|___/_|\_\ |___/\__, |_|                                                              
                           |___/                                     |_|                                                                
    ;                                                                                                                                   
    * this is faster and safer than 'distinct' or 'unique'?                                                                             
                                                                                                                                        
    proc sql;                                                                                                                           
      create                                                                                                                            
         table want as                                                                                                                  
      select                                                                                                                            
         max(x1) as x1                                                                                                                  
      from                                                                                                                              
         havsrt                                                                                                                         
      group                                                                                                                             
         by x1;                                                                                                                         
    ;quit;                                                                                                                              
                                                                                                                                        
     NOTE: Table WORK.WANT created, with 998674 rows and 1 columns.                                                                     
                                                                                                                                        
     256   ;quit;                                                                                                                       
     NOTE: PROCEDURE SQL used (Total process time):                                                                                     
           real time           5:32.24                                                                                                  
           cpu time            8:32.31                                                                                                  
                                                                                                                                        
    *_        _               _                                                                                                         
    | |__    | |__   __ _ ___| |__    _   _ _ __   __ _ _ __ ___  _   _ _ __                                                            
    | '_ \   | '_ \ / _` / __| '_ \  | | | | '_ \ / _` | '__/ _ \| | | | '_ \                                                           
    | |_) |  | | | | (_| \__ \ | | | | |_| | | | | (_| | | | (_) | |_| | |_) |                                                          
    |_.__(_) |_| |_|\__,_|___/_| |_|  \__,_|_| |_|\__, |_|  \___/ \__,_| .__/                                                           
                                                  |___/                |_|                                                              
    ;                                                                                                                                   
    sasfile havsrt load;                                                                                                                
                                                                                                                                        
    data _null_;                                                                                                                        
      declare hash H(dataset:"havSrt", hashexp:20);                                                                                     
      H.defineKey('x1');                                                                                                                
      H.defineDone();                                                                                                                   
      H.output(dataset:'want'); /* ~9MB */                                                                                              
      stop;                                                                                                                             
      set havSrt(keep=x1);                                                                                                              
    run;                                                                                                                                
                                                                                                                                        
    sasfile havSrt close;                                                                                                               
                                                                                                                                        
    * hash is single threaded and I have a slow uni-processor;                                                                          
    NOTE: There were 604461078 observations read from the data set WORK.HAVSRT.                                                         
    NOTE: The data set WORK.WANT has 998674 observations and 1 variables.                                                               
    NOTE: DATA statement used (Total process time):                                                                                     
          real time           1:56.29                                                                                                   
          cpu time            1:56.21                                                                                                   
                                                                                                                                        
    *         _               _                                            _                                                            
      ___    | |__   __ _ ___| |__     __ _ _ __ ___  _   _ _ __   ___  __| |                                                           
     / __|   | '_ \ / _` / __| '_ \   / _` | '__/ _ \| | | | '_ \ / _ \/ _` |                                                           
    | (__ _  | | | | (_| \__ \ | | | | (_| | | | (_) | |_| | |_) |  __/ (_| |                                                           
     \___(_) |_| |_|\__,_|___/_| |_|  \__, |_|  \___/ \__,_| .__/ \___|\__,_|                                                           
                                      |___/                |_|                                                                          
    ;                                                                                                                                   
    sasfile have load;                                                                                                                  
                                                                                                                                        
    data _null_;                                                                                                                        
      declare hash H(dataset:"have", hashexp:20);                                                                                       
      H.defineKey('x1');                                                                                                                
      H.defineDone();                                                                                                                   
      H.output(dataset:'want'); /* ~9MB */                                                                                              
      stop;                                                                                                                             
      set have(keep=x1);                                                                                                                
    run;                                                                                                                                
                                                                                                                                        
    NOTE: There were 604461078 observations read from the data set WORK.HAVE.                                                           
    NOTE: The data set WORK.WANT has 998674 observations and 1 variables.                                                               
    NOTE: DATA statement used (Total process time):                                                                                     
          real time           35.73 seconds                                                                                             
          cpu time            35.70 seconds                                                                                             
                                                                                                                                        
    sasfile have close;                                                                                                                 
    *    _               _                         _ _      _                                                                           
      __| |    ___  __ _| |  _ __   __ _ _ __ __ _| | | ___| |                                                                          
     / _` |   / __|/ _` | | | '_ \ / _` | '__/ _` | | |/ _ \ |                                                                          
    | (_| |_  \__ \ (_| | | | |_) | (_| | | | (_| | | |  __/ |                                                                          
     \__,_(_) |___/\__, |_| | .__/ \__,_|_|  \__,_|_|_|\___|_|                                                                          
                      |_|   |_|                                                                                                         
    ;                                                                                                                                   
    * save macro in autocall library;                                                                                                   
    filename ft15f001 "c:\oto\_slice.sas";                                                                                              
    parmcards4;                                                                                                                         
    %macro _slice(beg,end);                                                                                                             
      %utlopts;                                                                                                                         
      libname nvm "f:/wrk" access=readonly;                                                                                             
      libname sd1 "d:/sd1";                                                                                                             
      proc sql;                                                                                                                         
        create                                                                                                                          
           table sd1.want&beg as                                                                                                        
        select                                                                                                                          
           max(x1) as x1                                                                                                                
        from                                                                                                                            
           nvm.havsrt(firstobs=&beg obs=&end)                                                                                           
        group                                                                                                                           
           by x1;                                                                                                                       
      ;quit;                                                                                                                            
    %mend _slice;                                                                                                                       
    ;;;;                                                                                                                                
    run;quit;                                                                                                                           
                                                                                                                                        
    * test the macro interactively;                                                                                                     
    * note you can highlight and hit RMB(submit) to compile macro in your interactive session                                           
      then highlight and RMB the code below to test;                                                                                    
                                                                                                                                        
    * test interactively;                                                                                                               
                                                                                                                                        
    %*_slice(1,1000000);                                                                                                                
                                                                                                                                        
                                                                                                                                        
    * 16 parallel tasls (max system utilization is about 80% 12 cores at 100%;                                                          
    %let _s=%qsysfunc(compbl(&_r\PROGRA~1\SASHome\SASFoundation\9.4\sas.exe -sysin nul -log nul -work f\wrk                             
            -rsasuser -nosplash -sasautos &_r\oto -config &_r\cfg\cfgsas94m6.cfg));                                                     
                                                                                                                                        
    options noxwait noxsync;                                                                                                            
    %let tym=%sysfunc(time());                                                                                                          
    systask kill sys1 sys2 sys3 sys4  sys5 sys6 sys7 sys8 sys9 sys10 sys11 sys12 sys13 sys14 sys15 sys16;                               
    systask command "&_s -termstmt %nrstr(%_slice(1,37499999);) -log d:\log\a1.log" taskname=sys1;                                      
    systask command "&_s -termstmt %nrstr(%_slice(37500000,74999999);) -log d:\log\a2.log" taskname=sys2;                               
    systask command "&_s -termstmt %nrstr(%_slice(75000000,112499999);) -log d:\log\a3.log" taskname=sys3;                              
    systask command "&_s -termstmt %nrstr(%_slice(112500000,149999999);) -log d:\log\a4.log" taskname=sys4;                             
    systask command "&_s -termstmt %nrstr(%_slice(150000000,187499999);) -log d:\log\a5.log" taskname=sys5;                             
    systask command "&_s -termstmt %nrstr(%_slice(187500000,224999999);) -log d:\log\a6.log" taskname=sys6;                             
    systask command "&_s -termstmt %nrstr(%_slice(225000000,262499999);) -log d:\log\a7.log" taskname=sys7;                             
    systask command "&_s -termstmt %nrstr(%_slice(262500000,299999999);) -log d:\log\a8.log" taskname=sys8;                             
    systask command "&_s -termstmt %nrstr(%_slice(300000000,337499999);) -log d:\log\a9.log" taskname=sys9;                             
    systask command "&_s -termstmt %nrstr(%_slice(337500000,374999999);) -log d:\log\a10.log" taskname=sys10;                           
    systask command "&_s -termstmt %nrstr(%_slice(375000000,412499999);) -log d:\log\a11.log" taskname=sys11;                           
    systask command "&_s -termstmt %nrstr(%_slice(412500000,449999999);) -log d:\log\a12.log" taskname=sys12;                           
    systask command "&_s -termstmt %nrstr(%_slice(450000000,487499999);) -log d:\log\a13.log" taskname=sys13;                           
    systask command "&_s -termstmt %nrstr(%_slice(487500000,524999999);) -log d:\log\a14.log" taskname=sys14;                           
    systask command "&_s -termstmt %nrstr(%_slice(525000000,562499999);) -log d:\log\a15.log" taskname=sys15;                           
    systask command "&_s -termstmt %nrstr(%_slice(562500000,600000000);) -log d:\log\a16.log" taskname=sys16;                           
    waitfor sys1 sys2 sys3 sys4  sys5 sys6 sys7 sys8 sys9 sys10 sys11 sys12 sys13 sys14 sys15 sys16;                                    
    %put %sysevalf( %sysfunc(time()) - &tym);                                                                                           
                                                                                                                                        
    * VIEW to put back together;                                                                                                        
    data humptybackx;                                                                                                                   
      set                                                                                                                               
       sd1.want1                                                                                                                        
       sd1.want37500000                                                                                                                 
       sd1.want75000000                                                                                                                 
       sd1.want112500000                                                                                                                
       sd1.want150000000                                                                                                                
       sd1.want187500000                                                                                                                
       sd1.want225000000                                                                                                                
       sd1.want262500000                                                                                                                
       sd1.want300000000                                                                                                                
       sd1.want337500000                                                                                                                
       sd1.want375000000                                                                                                                
       sd1.want412500000                                                                                                                
       sd1.want450000000                                                                                                                
       sd1.want487500000                                                                                                                
       sd1.want525000000                                                                                                                
       sd1.want562500000;                                                                                                               
      by x1;                                                                                                                            
      if first.x1;                                                                                                                      
    run;quit;                                                                                                                           
                                                                                                                                        
    *_  __   _   _       _                                                                                                              
    / |/ /_ | |_| |__   | | ___   __ _                                                                                                  
    | | '_ \| __| '_ \  | |/ _ \ / _` |                                                                                                 
    | | (_) | |_| | | | | | (_) | (_| |                                                                                                 
    |_|\___/ \__|_| |_| |_|\___/ \__, |                                                                                                 
                                 |___/                                                                                                  
    ;                                                                                                                                   
                                                                                                                                        
    MPRINT(_SLICE):   create table sd1.want562500000 as select max(x1) as x1                                                            
    from nvm.havsrt(firstobs=562500000 obs=600000000) group by                                                                          
    x1;                                                                                                                                 
    NOTE: SAS threaded sort was used.                                                                                                   
    NOTE: Table SD1.WANT562500000 created, with 986378 rows and 1 columns.                                                              
                                                                                                                                        
    MPRINT(_SLICE):   ;                                                                                                                 
    MPRINT(_SLICE):  quit;                                                                                                              
    NOTE: PROCEDURE SQL used (Total process time):                                                                                      
          real time           32.55 seconds                                                                                             
          user cpu time       46.04 seconds                                                                                             
          system cpu time     1.78 seconds                                                                                              
          memory              1516833.62k                                                                                               
          OS Memory           1525008.00k                                                                                               
          Timestamp           05/14/2020 06:41:45 AM                                                                                    
          Step Count                        1  Switch Count  0                                                                          
                                                                                                                                        
    *_                           _           _                                                                                          
    | |__  _   _ _ __ ___  _ __ | |_ _   _  | | ___   __ _                                                                              
    | '_ \| | | | '_ ` _ \| '_ \| __| | | | | |/ _ \ / _` |                                                                             
    | | | | |_| | | | | | | |_) | |_| |_| | | | (_) | (_| |                                                                             
    |_| |_|\__,_|_| |_| |_| .__/ \__|\__, | |_|\___/ \__, |                                                                             
                          |_|        |___/           |___/                                                                              
    ;                                                                                                                                   
    462   data humptybackx;                                                                                                             
    463     set                                                                                                                         
    464      sd1.want1                                                                                                                  
    465      sd1.want37500000                                                                                                           
    466      sd1.want75000000                                                                                                           
    467      sd1.want112500000                                                                                                          
    468      sd1.want150000000                                                                                                          
    469      sd1.want187500000                                                                                                          
    470      sd1.want225000000                                                                                                          
    471      sd1.want262500000                                                                                                          
    472      sd1.want300000000                                                                                                          
    473      sd1.want337500000                                                                                                          
    474      sd1.want375000000                                                                                                          
    475      sd1.want412500000                                                                                                          
    476      sd1.want450000000                                                                                                          
    477      sd1.want487500000                                                                                                          
    478      sd1.want525000000                                                                                                          
    479      sd1.want562500000;                                                                                                         
    480     by x1;                                                                                                                      
    481     if first.x1;                                                                                                                
    482   run;                                                                                                                          
                                                                                                                                        
    NOTE: There were 986318 observations read from the data set SD1.WANT1.                                                              
    NOTE: There were 986186 observations read from the data set SD1.WANT37500000.                                                       
    NOTE: There were 986257 observations read from the data set SD1.WANT75000000.                                                       
    NOTE: There were 986346 observations read from the data set SD1.WANT112500000.                                                      
    NOTE: There were 986318 observations read from the data set SD1.WANT150000000.                                                      
    NOTE: There were 986265 observations read from the data set SD1.WANT187500000.                                                      
    NOTE: There were 986285 observations read from the data set SD1.WANT225000000.                                                      
    NOTE: There were 986322 observations read from the data set SD1.WANT262500000.                                                      
    NOTE: There were 986064 observations read from the data set SD1.WANT300000000.                                                      
    NOTE: There were 986313 observations read from the data set SD1.WANT337500000.                                                      
    NOTE: There were 986232 observations read from the data set SD1.WANT375000000.                                                      
    NOTE: There were 986179 observations read from the data set SD1.WANT412500000.                                                      
    NOTE: There were 986240 observations read from the data set SD1.WANT450000000.                                                      
    NOTE: There were 986221 observations read from the data set SD1.WANT487500000.                                                      
    NOTE: There were 986358 observations read from the data set SD1.WANT525000000.                                                      
    NOTE: There were 986378 observations read from the data set SD1.WANT562500000.                                                      
                                                                                                                                        
    NOTE: The data set WORK.HUMPTYBACKX has 998674 observations and 1 variables.                                                        
    NOTE: DATA statement used (Total process time):                                                                                     
          real time           6.26 seconds                                                                                              
          user cpu time       6.01 seconds                                                                                              
          system cpu time     0.20 seconds                                                                                              
          memory              26422.96k                                                                                                 
          OS Memory           28588680.00k                                        1                                                     
          Timestamp           05/14/2020 06:42:45 AM                              37500000                                              
          Step Count                        63  Switch Count  0                   75000000                                              
