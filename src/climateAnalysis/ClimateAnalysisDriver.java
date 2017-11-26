package climateAnalysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ClimateAnalysisDriver extends Configured implements Tool {

	  public static final Log log = LogFactory.getLog(ClimateAnalysisDriver.class);
	  private static Configuration conf;
	  private static Configuration etaConf;
	  private static Configuration marksimConf;
	  private static Configuration precisAndesConf;
	  private Job etaClimateJob, marksimClimateJob, precisAndesClimateJob;
	  public static String forecastDateOne, forecastDateTwo;
	  
	  public enum EtaDirPathsEnum{
		  ETASOUTHAMERICA_HADCM_CNTRL_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_cntrl/20min/hadcm_cntrl_baseline_1970s_prec_20min_sa_eta_asc"),
	      ETASOUTHAMERICA_HADCM_CNTRL_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_cntrl/20min/hadcm_cntrl_baseline_1970s_tmax_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_CNTRL_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_cntrl/20min/hadcm_cntrl_baseline_1970s_tmean_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_CNTRL_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_cntrl/20min/hadcm_cntrl_baseline_1970s_tmin_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_HIGH_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_high/20min/hadcm_high_baseline_1970s_prec_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_HIGH_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_high/20min/hadcm_high_baseline_1970s_tmax_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_HIGH_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_high/20min/hadcm_high_baseline_1970s_tmean_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_HIGH_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_high/20min/hadcm_high_baseline_1970s_tmin_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_LOW_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_low/20min/hadcm_low_baseline_1970s_prec_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_LOW_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_low/20min/hadcm_low_baseline_1970s_tmax_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_LOW_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_low/20min/hadcm_low_baseline_1970s_tmean_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_LOW_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_low/20min/hadcm_low_baseline_1970s_tmin_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_MIDI_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_midi/20min/hadcm_midi_baseline_1970s_prec_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_MIDI_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_midi/20min/hadcm_midi_baseline_1970s_tmax_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_MIDI_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_midi/20min/hadcm_midi_baseline_1970s_tmean_20min_sa_eta_asc"),
		  ETASOUTHAMERICA_HADCM_MIDI_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/hadcm_midi/20min/hadcm_midi_baseline_1970s_tmin_20min_sa_eta_asc");
		  
		  private final String pathValue;
		  
		  EtaDirPathsEnum( String pathValue){
			  this.pathValue = pathValue;
		  }
		  
		  public Path getPath(){ 
			  return new Path(pathValue); 
		  }
	  }
	  
	  public enum MarksimDirPathsEnum{
		  	  PATTERN_SCALING_MARKSIM_BASELINE_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/baseline/5min/baseline_baseline_2000s_prec_5min_no_tile_asc"),
		  	  PATTERN_SCALING_MARKSIM_BASELINE_RAINYDAYS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/baseline/5min/baseline_baseline_2000s_rainydays_5min_no_tile_asc"),
		  	  PATTERN_SCALING_MARKSIM_BASELINE_SOLAR_RADIATION_AT_GROUND("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/baseline/5min/baseline_baseline_2000s_solar_radiation_5min_no_tile_asc"),
		  	  PATTERN_SCALING_MARKSIM_BASELINE_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/baseline/5min/baseline_baseline_2000s_tmax_5min_no_tile_asc"),
		  	  PATTERN_SCALING_MARKSIM_BASELINE_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/baseline/5min/baseline_baseline_2000s_tmin_5min_no_tile_asc");
		  
		  private final String pathValue;
		  
		  MarksimDirPathsEnum( String pathValue){
			  this.pathValue = pathValue;
		  }
		  public Path getPath(){ 
			  return new Path(pathValue); 
		  }
	  }
	  public enum PrecisAndesDirPathsEnum{
		  	PRECIS_ANDES_ECHAM5_BIO("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_bio_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_CLOUDAM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_cloudam_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_EVCR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_evcr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_EVPOTF1("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_evpotf1_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_EVPOTF2("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_evpotf2_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_EVPOTR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_evpotr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_EVSS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_evss_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_prec_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_PRESS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_press_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_RHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_rhum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_SHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_shum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_SLHEAT("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_slheat_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_SOILMAF("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_soilmaf_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_SOILMRZ("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_soilmrz_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_SUBSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_subsr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_tmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_tmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_tmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_TRANSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_transr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_TSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_tsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_TSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_tsmmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_TSMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_tsmmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_WSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_wsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_ECHAM5_WSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/echam5/25min/echam5_baseline_1970s_wsmmax_25min_andes_precis_asc"),
		  	
		  	PRECIS_ANDES_HADAM3P_3_BIO("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_bio_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_CLOUDAM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_cloudam_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_EVCR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_evcr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_EVPOTF1("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_evpotf1_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_EVPOTF2("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_evpotf2_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_EVPOTR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_evpotr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_EVSS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_evss_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_prec_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_PRESS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_press_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_RHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_rhum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_SHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_shum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_SLHEAT("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_slheat_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_SOILMAF("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_soilmaf_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_SOILMRZ("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_soilmrz_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_SUBSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_subsr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_tmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_tmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_tmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_TRANSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_transr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_TSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_tsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_TSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_tsmmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_TSMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_tsmmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_WSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_wsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADAM3P_3_WSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadam3p_3/25min/hadam3p_3_baseline_1970s_wsmmax_25min_andes_precis_asc"),
		  	
		  	PRECIS_ANDES_HADCM3Q0_BIO("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_bio_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_CLOUDAM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_cloudam_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_EVCR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_evcr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_EVPOTF1("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_evpotf1_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_EVPOTF2("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_evpotf2_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_EVPOTR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_evpotr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_EVSS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_evss_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_prec_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_PRESS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_press_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_RHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_rhum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_SHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_shum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_SLHEAT("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_slheat_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_SOILMAF("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_soilmaf_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_SOILMRZ("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_soilmrz_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_SUBSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_subsr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_tmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_tmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_tmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_TRANSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_transr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_TSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_tsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_TSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_tsmmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_TSMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_tsmmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_WSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_wsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q0_WSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q0/25min/hadcm3q0_baseline_1970s_wsmmax_25min_andes_precis_asc"),
		  	
		  	PRECIS_ANDES_HADCM3Q3_BIO("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_bio_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_CLOUDAM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_cloudam_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_EVCR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_evcr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_EVPOTF1("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_evpotf1_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_EVPOTF2("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_evpotf2_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_EVPOTR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_evpotr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_EVSS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_evss_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_prec_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_PRESS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_press_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_RHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_rhum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_SHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_shum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_SLHEAT("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_slheat_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_SOILMAF("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_soilmaf_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_SOILMRZ("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_soilmrz_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_SUBSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_subsr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_tmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_tmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_tmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_TRANSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_transr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_TSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_tsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_TSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_tsmmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_TSMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_tsmmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_WSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_wsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q3_WSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q3/25min/hadcm3q3_baseline_1970s_wsmmax_25min_andes_precis_asc"),
		  	
		  	PRECIS_ANDES_HADCM3Q16_BIO("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_bio_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_CLOUDAM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_cloudam_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_EVCR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_evcr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_EVPOTF1("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_evpotf1_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_EVPOTF2("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_evpotf2_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_EVPOTR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_evpotr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_EVSS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_evss_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_prec_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_PRESS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_press_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_RHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_rhum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_SHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_shum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_SLHEAT("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_slheat_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_SOILMAF("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_soilmaf_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_SOILMRZ("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_soilmrz_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_SUBSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_subsr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_tmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_tmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_tmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_TRANSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_transr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_TSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_tsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_TSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_tsmmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_TSMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_tsmmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_HADCM3Q16_WSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_wsmean_25min_andes_precis_asc"),  	
		  	PRECIS_ANDES_HADCM3Q16_WSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/hadcm3q16/25min/hadcm3q16_baseline_1970s_wsmmax_25min_andes_precis_asc"),
		  	
		  	PRECIS_ANDES_NCEP_R2_BIO("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_bio_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_CLOUDAM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_cloudam_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_EVCR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_evcr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_EVPOTF1("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_evpotf1_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_EVPOTF2("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_evpotf2_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_EVPOTR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_evpotr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_EVSS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_evss_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_PREC("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_prec_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_PRESS("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_press_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_RHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_rhum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_SHUM("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_shum_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_SLHEAT("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_slheat_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_SOILMAF("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_soilmaf_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_SOILMRZ("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_soilmrz_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_SUBSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_subsr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_TMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_tmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_TMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_tmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_TMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_tmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_TRANSR("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_transr_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_TSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_tsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_TSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_tsmmax_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_TSMIN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_tsmmin_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_WSMEAN("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_wsmean_25min_andes_precis_asc"),
		  	PRECIS_ANDES_NCEP_R2_WSMAX("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1990s/ncep_r2/25min/ncep_r2_baseline_1990s_wsmmax_25min_andes_precis_asc");
		  	
			  private final String pathValue;
			  
			  PrecisAndesDirPathsEnum( String pathValue){
				  this.pathValue = pathValue;
			  }
			  public Path getPath(){ 
				  return new Path(pathValue); 
			  }
	  }
	  public static class EtaClimateMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		  @Override
		  protected void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		  }
      }
	  public static class MarksimClimateMapper  extends Mapper<LongWritable, Text, Text, LongWritable>{	
         @Override
		 protected void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {   
		 }
	  }
	  public static class PrecisAndesClimateMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		  @Override
		  protected void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {   
		  }
	  }
	  public static class EtaClimateReducer extends Reducer<Text, LongWritable, Text,LongWritable> {
			 
			Configuration configuration = new Configuration();
			public String temp = new String();
			public String fileName = new String();
			final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
			public  LocalDate forecastDateOne, forecastDateTwo;
			public  LocalDate then = LocalDate.of( 1970, 1, 1 );
			public static final Log log = LogFactory.getLog(EtaClimateReducer.class);
			public DataPool dataPool, finalDataPool;
			public static int counter = 0;
			public static int counterFinalValue = 0;
			public Path tempDirPath = new Path("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/");
			public static ArrayList<DataPool> dataPoolArray = new ArrayList<>();
			private List<Path> dirPaths = new LinkedList<>();
			
			public void initializePaths(){
				for(EtaDirPathsEnum dirPath : EtaDirPathsEnum.values())
					dirPaths.add(dirPath.getPath());
			}
			public void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
					throws IOException {
				initializePaths();
				final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
				forecastDateOne= LocalDate.parse(this.configuration.get("forecastDateOne"), DATE_FORMAT);
				forecastDateTwo= LocalDate.parse(this.configuration.get("forecastDateTwo"), DATE_FORMAT);
				for(Path dirPath : dirPaths){
			        FileSystem fs = null;
					try {
						fs = FileSystem.get(new URI(dirPath.toString()), etaConf);
					} catch (URISyntaxException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
			        FileStatus[] fileStatus = fs.listStatus(dirPath);
				    if (fileStatus != null) {
					    for (FileStatus child : fileStatus) {
					    	fileName = child.getPath().getName();
					    	System.out.println("asdasdasdads          "+fileName);
					        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(child.getPath())));
					        String line;
					        line=br.readLine();
					        while (line != null){
					        	// value is space-separated values: 
					        	// ncols   xxx
					        	// nrows   xxx
					        	// xllcenter xxx 
					        	// yllcenter xxx
					        	// cellsize xxx
					        	// nodata_value xxx
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx  
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx  
					            // we project out (location, occurrences) so we can iterate through all values for a given cell
					            String[] split = line.split(" ");
					            if (EtaClimateReducer.dataPoolArray == null)
					            	EtaClimateReducer.dataPoolArray = new ArrayList<>();
					            if (split.length == 2) {
					            	if(split[0].equalsIgnoreCase("ncols")){
					            		dataPool.ncols = Integer.parseInt(split[1]);
					            		EtaClimateReducer.counter++;
					            
					                }else if (split[0].equalsIgnoreCase("nrows")){
					                	dataPool.nrows = Integer.parseInt(split[1]);
					                	EtaClimateReducer.counter++;
					         
					                }else if (split[0].equalsIgnoreCase("xllcenter")){
					                	dataPool.xllcenter = Double.parseDouble(split[1]);
					                	EtaClimateReducer.counter++;
					         
					                }else if (split[0].equalsIgnoreCase("yllcenter")){
					                	dataPool.yllcenter = Double.parseDouble(split[1]);
					                	EtaClimateReducer.counter++;
					            
					                }else if (split[0].equalsIgnoreCase("cellsize")){
					                	dataPool.cellsize = Double.parseDouble(split[1]);
					                	EtaClimateReducer.counter++;
					            
					                }else if (split[0].equalsIgnoreCase("nodata_value")){
					                	dataPool.nodata_value = Integer.parseInt(split[1]);
					                	EtaClimateReducer.counter++;
					            	
					                }
					           }else if(split.length > 2) {
					                	double[] valueLine = {};
					                	for (int x=0; x < dataPool.ncols ; x++){
					                		valueLine[x] = Double.parseDouble(split[x]);
					                	}
					                	this.dataPool.record.add(valueLine);
					                	EtaClimateReducer.counter++;
					           }
					           if(EtaClimateReducer.counter == this.dataPool.nrows + 6){
					            	EtaClimateReducer.dataPoolArray.add(this.dataPool);
					           }
					            line = br.readLine();
					        }
					    }
							// TODO Auto-generated method stub
					    	if(finalDataPool == null)
					    		finalDataPool = new DataPool();
					    	Period period = Period.between( then, forecastDateOne );
					    	int daysInTotal = period.getDays();
					    	int samplePeriodsInTotal = daysInTotal * 72;
						    double[] extractedValueList = {};
						    int i = 0;
						    for (int j = 0; j < EtaClimateReducer.dataPoolArray.get(0).nrows ; j++){
						    	for (int k = 0; k < EtaClimateReducer.dataPoolArray.get(0).ncols ; k++){
						    		for(DataPool dP : EtaClimateReducer.dataPoolArray){
						    			extractedValueList[i++] = dP.record.get(j)[k];
						    		}
		  				    		double lastSelectedValue = 0.0;
						    		for(int m = 0; m < extractedValueList.length; m++){
						    			if(extractedValueList[m] == -9999){
						    				boolean FOUND = false;
						    				 for(int n = 1; m + n <= extractedValueList.length && n <= m; n++ )
						    					 if( extractedValueList[m+n] != -9999){
						    						 extractedValueList[m] = extractedValueList[m+n];
						    				      FOUND = true;
						    					 }
						    					 else if(extractedValueList[m-n] != -9999){
						    						 extractedValueList[m] = extractedValueList[m-n];
						    					  FOUND = true;
						    					 }
						    				 if(FOUND) extractedValueList[m] = lastSelectedValue;
						    			}else{
						    				lastSelectedValue = extractedValueList[m];
						    			}
						    		}
								    double[] xData = {0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240};
								    CurveFitting curveFitting = new CurveFitting(xData, extractedValueList);
								    curveFitting.doFit(3);                                                         //      y = a + b * x + c * x^2 + d * x^3 + e * x^4
								    double[] resultParameters = curveFitting.getParams(); 
								    finalDataPool.ncols = EtaClimateReducer.dataPoolArray.get(0).ncols;
								    finalDataPool.nrows = EtaClimateReducer.dataPoolArray.get(0).nrows;
								    finalDataPool.xllcenter = EtaClimateReducer.dataPoolArray.get(0).xllcenter;
								    finalDataPool.yllcenter = EtaClimateReducer.dataPoolArray.get(0).yllcenter;
								    finalDataPool.cellsize = EtaClimateReducer.dataPoolArray.get(0).cellsize;
								    finalDataPool.nodata_value = EtaClimateReducer.dataPoolArray.get(0).nodata_value;
								    finalDataPool.record.get(j)[k] = resultParameters[0] + resultParameters[1] * samplePeriodsInTotal + resultParameters[2] * samplePeriodsInTotal * samplePeriodsInTotal +
								    		                              resultParameters[3] * samplePeriodsInTotal *samplePeriodsInTotal * samplePeriodsInTotal + 
								    		                              resultParameters[4] * samplePeriodsInTotal * samplePeriodsInTotal * samplePeriodsInTotal * samplePeriodsInTotal;
						    	}
						    }
						    
						    FileSystem hdfs = null;
						    temp = "hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/"+dirPath.toString().substring(tempDirPath.toString().length());
						    Path fileDir = new Path(temp);
							try {
								hdfs = FileSystem.get( new URI( "hdfs://localhost:9000/ClimateDataForecast"), etaConf );						
							} catch (URISyntaxException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							if ( hdfs.exists( fileDir )) { hdfs.delete( fileDir, true ); }
							hdfs.mkdirs(fileDir);
							temp += "/" + fileName.replaceAll("[0-9]+","") + "-Predicted for " + forecastDateOne.toString();
							Path absoluteFilePath = new Path(temp);				    
						    FSDataOutputStream os = hdfs.create( absoluteFilePath);
						    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );				   
						    br.write("ncols " + finalDataPool.ncols);
						    br.newLine();
						    br.write("nrows " + finalDataPool.nrows);
						    br.newLine();
						    br.write("xllcenter " + finalDataPool.xllcenter);
						    br.newLine();
						    br.write("yllcenter " + finalDataPool.yllcenter);
						    br.newLine();
						    br.write("nodata_value " + finalDataPool.nodata_value);
						    br.newLine();
						    for(int k=0; k < finalDataPool.nrows + 6 ; k++){
						    	for(int m = 0; m < finalDataPool.ncols; m++){				    
						    		br.write( String.valueOf(finalDataPool.record.get(k)[m]));
						    	}
						    	br.newLine();
						    }
						    br.close();
						    hdfs.close();
						    try {
								context.write(arg0, (LongWritable) arg1);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						    nullifyDataPool();
				    
				    } else {
							    // Handle the case where dir is not really a directory.
							    // Checking dir.isDirectory() above would not be sufficient
							    // to avoid race conditions with another process that deletes
							    // directories.
				    }
				}
			}
			public void nullifyDataPool(){
			  	this.dataPool.ncols = 0;
			  	this.dataPool.nrows = 0;
			  	this.dataPool.xllcenter = 0.0;
			  	this.dataPool.yllcenter = 0.0;
			  	this.dataPool.cellsize = 0.0;
			  	this.dataPool.nodata_value = 0;
			  	this.dataPool.record = null;
			  	EtaClimateReducer.dataPoolArray.clear();
			  	this.finalDataPool = null;
		    }
		}
 
	  public static class MarksimClimateReducer extends Reducer<Text, LongWritable, Text,LongWritable> {
	  	 
	  	Configuration configuration = new Configuration();
	  	public String temp = new String();
	  	public String fileName = new String();
	  	final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
	  	public  LocalDate forecastDateOne, forecastDateTwo;
	  	public  LocalDate then = LocalDate.of( 2000, 1, 1 );
	  	public static final Log log = LogFactory.getLog(MarksimClimateReducer.class);
	  	public DataPool dataPool, finalDataPool;
	  	public static int counter = 0;
	  	public static int counterFinalValue = 0;
	  	public Path tempDirPath = new Path("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/");
	  	public static ArrayList<DataPool> dataPoolArray = new ArrayList<>();
	  	private List<Path> dirPaths = new LinkedList<>();
	  	
	  	public void initializePaths(){
			for(MarksimDirPathsEnum dirPath : MarksimDirPathsEnum.values())
				dirPaths.add(dirPath.getPath());		
	  	}
	  	public void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
	  			throws IOException {
	  		initializePaths();
	  		final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
	  		forecastDateOne= LocalDate.parse(this.configuration.get("forecastDateOne"), DATE_FORMAT);
	  		forecastDateTwo= LocalDate.parse(this.configuration.get("forecastDateTwo"), DATE_FORMAT);
	  		for(Path dirPath : dirPaths){
	  	        FileSystem fs = null;
	  			try {
	  				fs = FileSystem.get(new URI(dirPath.toString()), marksimConf);
	  			} catch (URISyntaxException e1) {
	  				// TODO Auto-generated catch block
	  				e1.printStackTrace();
	  			}
	  	        FileStatus[] fileStatus = fs.listStatus(dirPath);
	  		    if (fileStatus != null) {
	  			    for (FileStatus child : fileStatus) {
	  			    	fileName = child.getPath().getName();
	  			    	System.out.println("asdasdasdads          "+fileName);
	  			        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(child.getPath())));
	  			        String line;
	  			        line = br.readLine();
	  			        while (line != null){
	  			        	// value is space-separated values: 
	  			        	// ncols   xxx
	  			        	// nrows   xxx
	  			        	// xllcenter xxx 
	  			        	// yllcenter xxx
	  			        	// cellsize xxx
	  			        	// nodata_value xxx
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx  
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx  
	  			            // we project out (location, occurrences) so we can iterate through all values for a given cell
	  			            String[] split = line.split(" ");
	  			            if (MarksimClimateReducer.dataPoolArray == null)
	  			            	MarksimClimateReducer.dataPoolArray = new ArrayList<>();
	  			            if (split.length == 2) {
	  			            	if(split[0].equalsIgnoreCase("ncols")){
	  			            		dataPool.ncols = Integer.parseInt(split[1]);
	  			            		MarksimClimateReducer.counter++;
	  			            
	  			                }else if (split[0].equalsIgnoreCase("nrows")){
	  			                	dataPool.nrows = Integer.parseInt(split[1]);
	  			                	MarksimClimateReducer.counter++;
	  			         
	  			                }else if (split[0].equalsIgnoreCase("xllcenter")){
	  			                	dataPool.xllcenter = Double.parseDouble(split[1]);
	  			                	MarksimClimateReducer.counter++;
	  			         
	  			                }else if (split[0].equalsIgnoreCase("yllcenter")){
	  			                	dataPool.yllcenter = Double.parseDouble(split[1]);
	  			                	MarksimClimateReducer.counter++;
	  			            
	  			                }else if (split[0].equalsIgnoreCase("cellsize")){
	  			                	dataPool.cellsize = Double.parseDouble(split[1]);
	  			                	MarksimClimateReducer.counter++;
	  			            
	  			                }else if (split[0].equalsIgnoreCase("nodata_value")){
	  			                	dataPool.nodata_value = Integer.parseInt(split[1]);
	  			                	MarksimClimateReducer.counter++;
	  			            	
	  			                }
	  			           }else if(split.length > 2) {
	  			                	double[] valueLine = {};
	  			                	for (int x=0; x < dataPool.ncols ; x++){
	  			                		valueLine[x] = Double.parseDouble(split[x]);
	  			                	}
	  			                	this.dataPool.record.add(valueLine);
	  			                	MarksimClimateReducer.counter++;
	  			           }
	  			           if(MarksimClimateReducer.counter == this.dataPool.nrows + 6){
	  			            	MarksimClimateReducer.dataPoolArray.add(this.dataPool);
	  			           }
	  			            line = br.readLine();
	  			        }
	  			    }
	  					// TODO Auto-generated method stub
	  			    	if(finalDataPool == null)
	  			    		finalDataPool = new DataPool();
	  			    	Period period = Period.between( then, forecastDateOne );
	  			    	int daysInTotal = period.getDays();
	  			    	int samplePeriodsInTotal = daysInTotal * 288;
	  				    double[] values = {};
	  				    double[] extractedValueList = {};
	  				    int i = 0;
	  				    for (int j = 0; j < MarksimClimateReducer.dataPoolArray.get(0).nrows ; j++){
	  				    	for (int k = 0; k < MarksimClimateReducer.dataPoolArray.get(0).ncols ; k++){
	  				    		for(DataPool dP : MarksimClimateReducer.dataPoolArray){
	  				    			values = Arrays.copyOf(dP.record.get(j), dP.record.get(j).length);
	  				    			extractedValueList[i++] = values[k];
	  				    		}
	  				    		double lastSelectedValue = 0.0;
					    		for(int m = 0; m < extractedValueList.length; m++){
					    			if(extractedValueList[m] == -9999){
					    				boolean FOUND = false;
					    				 for(int n = 1; m + n <= extractedValueList.length && n <= m; n++ )
					    					 if( extractedValueList[m+n] != -9999){
					    						 extractedValueList[m] = extractedValueList[m+n];
					    				      FOUND = true;
					    					 }
					    					 else if(extractedValueList[m-n] != -9999){
					    						 extractedValueList[m] = extractedValueList[m-n];
					    					  FOUND = true;
					    					 }
					    				 if(FOUND) extractedValueList[m] = lastSelectedValue;
					    			}else{
					    				lastSelectedValue = extractedValueList[m];
					    			}
					    		}
	  						    double[] xData = {0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240};
	  						    CurveFitting curveFitting = new CurveFitting(xData, extractedValueList);
	  						    curveFitting.doFit(3);                                                         //      y = a + b * x + c * x^2 + d * x^3 + e * x^4
	  						    double[] resultParameters = curveFitting.getParams(); 
	  						    finalDataPool.ncols = MarksimClimateReducer.dataPoolArray.get(0).ncols;
	  						    finalDataPool.nrows = MarksimClimateReducer.dataPoolArray.get(0).nrows;
	  						    finalDataPool.xllcenter = MarksimClimateReducer.dataPoolArray.get(0).xllcenter;
	  						    finalDataPool.yllcenter = MarksimClimateReducer.dataPoolArray.get(0).yllcenter;
	  						    finalDataPool.cellsize = MarksimClimateReducer.dataPoolArray.get(0).cellsize;
	  						    finalDataPool.nodata_value = MarksimClimateReducer.dataPoolArray.get(0).nodata_value;
	  						    finalDataPool.record.get(j)[k] = resultParameters[0] + resultParameters[1] * samplePeriodsInTotal + resultParameters[2] * samplePeriodsInTotal * samplePeriodsInTotal +
	  						    		                              resultParameters[3] * samplePeriodsInTotal *samplePeriodsInTotal * samplePeriodsInTotal + 
	  						    		                              resultParameters[4] * samplePeriodsInTotal * samplePeriodsInTotal * samplePeriodsInTotal * samplePeriodsInTotal;
	  				    	}
	  				    }
	  				    
	  				    FileSystem hdfs = null;
	  				    temp = "hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/"+dirPath.toString().substring(tempDirPath.toString().length());
	  					Path fileDir = new Path(temp);
	  				    try {
	  						hdfs = FileSystem.get( new URI( "hdfs://localhost:9000/ClimateDataForecast" ), marksimConf);
	  					} catch (URISyntaxException e1) {
	  						// TODO Auto-generated catch block
	  						e1.printStackTrace();
	  					}
	  					if ( hdfs.exists( fileDir )) { hdfs.delete( fileDir, true ); }
	  					hdfs.mkdirs(fileDir);
	  					temp += "/" + fileName.replaceAll("[0-9]+","") + "-Predicted for " + forecastDateOne.toString();
	  					Path absoluteFilePath = new Path(temp);				    
	  				    FSDataOutputStream os = hdfs.create( absoluteFilePath);
	  				    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );				   
	  				    br.write("ncols " + finalDataPool.ncols);
	  				    br.newLine();
	  				    br.write("nrows " + finalDataPool.nrows);
	  				    br.newLine();
	  				    br.write("xllcenter " + finalDataPool.xllcenter);
	  				    br.newLine();
	  				    br.write("yllcenter " + finalDataPool.yllcenter);
	  				    br.newLine();
	  				    br.write("nodata_value " + finalDataPool.nodata_value);
	  				    br.newLine();
	  				    for(int k=0; k < finalDataPool.nrows + 6 ; k++){
	  				    	for(int m = 0; m < finalDataPool.ncols; m++){				    
	  				    		br.write( String.valueOf(finalDataPool.record.get(k)[m]));
	  				    	}
	  				    	br.newLine();
	  				    }
	  				    br.close();
	  				    hdfs.close();
	  				    try {
	  						context.write(arg0, (LongWritable) arg1);
	  					} catch (InterruptedException e) {
	  						// TODO Auto-generated catch block
	  						e.printStackTrace();
	  					}
	  				    nullifyDataPool();
	  		    
	  		    } else {
	  					    // Handle the case where dir is not really a directory.
	  					    // Checking dir.isDirectory() above would not be sufficient
	  					    // to avoid race conditions with another process that deletes
	  					    // directories.
	  		    }
	  		}
	  	}
	  	public void nullifyDataPool(){
	  		  	this.dataPool.ncols = 0;
	  		  	this.dataPool.nrows = 0;
	  		  	this.dataPool.xllcenter = 0.0;
	  		  	this.dataPool.yllcenter = 0.0;
	  		  	this.dataPool.cellsize = 0.0;
	  		  	this.dataPool.nodata_value = 0;
	  		  	this.dataPool.record = null;
	  		  	MarksimClimateReducer.dataPoolArray.clear();
	  		  	this.finalDataPool = null;
	  	}
	  }

	  public static class PrecisAndesClimateReducer extends Reducer<Text, LongWritable, Text,LongWritable> {
	  	 
	  	Configuration configuration = new Configuration();
	  	public String temp = new String();
	  	public String fileName = new String();
	  	final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
	  	public  LocalDate forecastDateOne, forecastDateTwo;
	  	public LocalDate then1 = LocalDate.of( 1970, 1, 1 );
	  	public LocalDate then2 = LocalDate.of( 1990, 1, 1 );
	  	public static final Log log = LogFactory.getLog(PrecisAndesClimateReducer.class);
	  	public DataPool dataPool, finalDataPool;
	  	public static int counter = 0;
	  	public static int counterFinalValue = 0;
	  	public Path tempDirPath = new Path("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/");
	  	public static ArrayList<DataPool> dataPoolArray = new ArrayList<>();
	  	

	  	
	  	private List<Path> dirPaths = new LinkedList<>();
	  	
	  	public void initializePaths(){
			for(PrecisAndesDirPathsEnum dirPath : PrecisAndesDirPathsEnum.values())
				dirPaths.add(dirPath.getPath());
	  	}	
	  	public void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
	  			throws IOException {
	  		initializePaths();
	  		final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
	  		forecastDateOne= LocalDate.parse(this.configuration.get("forecastDateOne"), DATE_FORMAT);
	  		forecastDateTwo= LocalDate.parse(this.configuration.get("forecastDateTwo"), DATE_FORMAT);
	  		for(Path dirPath : dirPaths){
	  	        FileSystem fs = null;
	  			try {
	  				fs = FileSystem.get(new URI(dirPath.toString()), precisAndesConf);
	  			} catch (URISyntaxException e1) {
	  				// TODO Auto-generated catch block
	  				e1.printStackTrace();
	  			}
	  	        FileStatus[] fileStatus = fs.listStatus(dirPath);
	  		    if (fileStatus != null) {
	  			    for (FileStatus child : fileStatus) {
	  			    	fileName = child.getPath().getName();
	  			    	System.out.println("asdasdasdads          "+fileName);
	  			        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(child.getPath())));
	  			        String line;
	  			        line=br.readLine();
	  			        while (line != null){
	  			        	// value is space-separated values: 
	  			        	// ncols   xxx
	  			        	// nrows   xxx
	  			        	// xllcenter xxx 
	  			        	// yllcenter xxx
	  			        	// cellsize xxx
	  			        	// nodata_value xxx
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx  
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
	  			        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx  
	  			            // we project out (location, occurrences) so we can iterate through all values for a given cell
	  			            String[] split = line.split(" ");
	  			            if (PrecisAndesClimateReducer.dataPoolArray == null)
	  			            	PrecisAndesClimateReducer.dataPoolArray = new ArrayList<>();
	  			            if (split.length == 2) {
	  			            	if(split[0].equalsIgnoreCase("ncols")){
	  			            		dataPool.ncols = Integer.parseInt(split[1]);
	  			            		PrecisAndesClimateReducer.counter++;
	  			            
	  			                }else if (split[0].equalsIgnoreCase("nrows")){
	  			                	dataPool.nrows = Integer.parseInt(split[1]);
	  			                	PrecisAndesClimateReducer.counter++;
	  			         
	  			                }else if (split[0].equalsIgnoreCase("xllcenter")){
	  			                	dataPool.xllcenter = Double.parseDouble(split[1]);
	  			                	PrecisAndesClimateReducer.counter++;
	  			         
	  			                }else if (split[0].equalsIgnoreCase("yllcenter")){
	  			                	dataPool.yllcenter = Double.parseDouble(split[1]);
	  			                	PrecisAndesClimateReducer.counter++;
	  			            
	  			                }else if (split[0].equalsIgnoreCase("cellsize")){
	  			                	dataPool.cellsize = Double.parseDouble(split[1]);
	  			                	PrecisAndesClimateReducer.counter++;
	  			            
	  			                }else if (split[0].equalsIgnoreCase("nodata_value")){
	  			                	dataPool.nodata_value = Integer.parseInt(split[1]);
	  			                	PrecisAndesClimateReducer.counter++;
	  			            	
	  			                }
	  			           }else if(split.length > 2) {
	  			                	double[] valueLine = {};
	  			                	for (int x=0; x < dataPool.ncols ; x++){
	  			     
	  			              			valueLine[x] = Double.parseDouble(split[x]);
	  			                 			                			
	  			                	}
	  			                	this.dataPool.record.add(valueLine);
	  			                	PrecisAndesClimateReducer.counter++;
	  			           }
	  			           if(PrecisAndesClimateReducer.counter == this.dataPool.nrows + 6){
	  			            	PrecisAndesClimateReducer.dataPoolArray.add(this.dataPool);
	  			           }
	  			            line=br.readLine();
	  			        }
	  			    }
	  					// TODO Auto-generated method stub
	  			    	if(finalDataPool == null)
	  			    		finalDataPool = new DataPool();
	  			    	Period period = Period.between( then1, forecastDateOne );
	  			    	int daysInTotal = period.getDays();
	  			    	int samplePeriodsInTotal = daysInTotal * 72;
	  				    double[] values = {};
	  				    double[] extractedValueList = {};
	  				    int i = 0;
	  				    for (int j = 0; j < PrecisAndesClimateReducer.dataPoolArray.get(0).nrows ; j++){
	  				    	for (int k = 0; k < PrecisAndesClimateReducer.dataPoolArray.get(0).ncols ; k++){
	  				    		for(DataPool dP : PrecisAndesClimateReducer.dataPoolArray){
	  				    			values = Arrays.copyOf(dP.record.get(j), dP.record.get(j).length);
	  				    			extractedValueList[i++] = values[k];
	  				    		}
	  				    		double lastSelectedValue = 0.0;
					    		for(int m = 0; m < extractedValueList.length; m++){
					    			if(extractedValueList[m] == -9999){
					    				boolean FOUND = false;
					    				 for(int n = 1; m + n <= extractedValueList.length && n <= m; n++ )
					    					 if( extractedValueList[m+n] != -9999){
					    						 extractedValueList[m] = extractedValueList[m+n];
					    				      FOUND = true;
					    					 }
					    					 else if(extractedValueList[m-n] != -9999){
					    						 extractedValueList[m] = extractedValueList[m-n];
					    					  FOUND = true;
					    					 }
					    				 if(FOUND) extractedValueList[m] = lastSelectedValue;
					    			}else{
					    				lastSelectedValue = extractedValueList[m];
					    			}
					    		}
	  						    double[] xData = {0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240};
	  						    CurveFitting curveFitting = new CurveFitting(xData, extractedValueList);
	  						    curveFitting.doFit(3);                                                         //      y = a + b * x + c * x^2 + d * x^3 + e * x^4
	  						    double[] resultParameters = curveFitting.getParams(); 
	  						    finalDataPool.ncols = PrecisAndesClimateReducer.dataPoolArray.get(0).ncols;
	  						    finalDataPool.nrows = PrecisAndesClimateReducer.dataPoolArray.get(0).nrows;
	  						    finalDataPool.xllcenter = PrecisAndesClimateReducer.dataPoolArray.get(0).xllcenter;
	  						    finalDataPool.yllcenter = PrecisAndesClimateReducer.dataPoolArray.get(0).yllcenter;
	  						    finalDataPool.cellsize = PrecisAndesClimateReducer.dataPoolArray.get(0).cellsize;
	  						    finalDataPool.nodata_value = PrecisAndesClimateReducer.dataPoolArray.get(0).nodata_value;
	  						    finalDataPool.record.get(j)[k] = resultParameters[0] + resultParameters[1] * samplePeriodsInTotal + resultParameters[2] * samplePeriodsInTotal * samplePeriodsInTotal +
	  						    		                              resultParameters[3] * samplePeriodsInTotal *samplePeriodsInTotal * samplePeriodsInTotal + 
	  						    		                              resultParameters[4] * samplePeriodsInTotal * samplePeriodsInTotal * samplePeriodsInTotal * samplePeriodsInTotal;
	  				    	}
	  				    }
	  				    
	  				    FileSystem hdfs = null;
	  				    ;
	  				    temp =  "hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/"+dirPath.toString().substring(tempDirPath.toString().length());
	  					Path fileDir = new Path(temp);
	  				    try {
	  						hdfs = FileSystem.get( new URI( "hdfs://localhost:9000/ClimateDataForecast" ), precisAndesConf );
	  					} catch (URISyntaxException e1) {
	  						// TODO Auto-generated catch block
	  						e1.printStackTrace();
	  					}
	  					if ( hdfs.exists( fileDir )) { hdfs.delete( fileDir, true ); }
	  					hdfs.mkdirs(fileDir);
	  					temp += "/" + fileName.replaceAll("[0-9]+","") + "-Predicted for " + forecastDateOne.toString();
	  					Path absoluteFilePath = new Path(temp);				    
	  				    FSDataOutputStream os = hdfs.create( absoluteFilePath);
	  				    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );				   
	  				    br.write("ncols " + finalDataPool.ncols);
	  				    br.newLine();
	  				    br.write("nrows " + finalDataPool.nrows);
	  				    br.newLine();
	  				    br.write("xllcenter " + finalDataPool.xllcenter);
	  				    br.newLine();
	  				    br.write("yllcenter " + finalDataPool.yllcenter);
	  				    br.newLine();
	  				    br.write("nodata_value " + finalDataPool.nodata_value);
	  				    br.newLine();
	  				    for(int k=0; k < finalDataPool.nrows + 6 ; k++){
	  				    	for(int m = 0; m < finalDataPool.ncols; m++){				    
	  				    		br.write( String.valueOf(finalDataPool.record.get(k)[m]));
	  				    	}
	  				    	br.newLine();
	  				    }
	  				    br.close();
	  				    hdfs.close();
	  				    try {
	  						context.write(arg0, (LongWritable) arg1);
	  					} catch (InterruptedException e) {
	  						// TODO Auto-generated catch block
	  						e.printStackTrace();
	  					}
	  				    nullifyDataPool();
	  		    
	  		    } else {
	  					    // Handle the case where dir is not really a directory.
	  					    // Checking dir.isDirectory() above would not be sufficient
	  					    // to avoid race conditions with another process that deletes
	  					    // directories.
	  		    }
	  		}
	  	}
	  	public void nullifyDataPool(){
	  		  	this.dataPool.ncols = 0;
	  		  	this.dataPool.nrows = 0;
	  		  	this.dataPool.xllcenter = 0.0;
	  		  	this.dataPool.yllcenter = 0.0;
	  		  	this.dataPool.cellsize = 0.0;
	  		  	this.dataPool.nodata_value = 0;
	  		  	this.dataPool.record = null;
	  		  	PrecisAndesClimateReducer.dataPoolArray.clear();
	  		  	this.finalDataPool = null;
	  	}
	  }
	  @SuppressWarnings({"deprecation" })
	public int start(Path[] etaInputDir, Path[] precisAndesInputDir, Path marksimInputDir, Path outputDir) throws IOException {

		    log.info("Creating configuration for Eta Climate Job");
		    ClimateAnalysisDriver.etaConf = new Configuration();
		    Path etaClimateOutput = new Path("hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/");

		    this.etaClimateJob = null;
			try {
				this.etaClimateJob = new Job(ClimateAnalysisDriver.etaConf, "etaClimate");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ClimateAnalysisDriver.etaConf.set("fs.hdfs.impl", 
			          org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			  
			ClimateAnalysisDriver.etaConf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName());
			ClimateAnalysisDriver.etaConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/core-site.xml"));
			ClimateAnalysisDriver.etaConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));
			this.etaClimateJob.setJarByClass(ClimateAnalysisDriver.class);
			this.etaClimateJob.setJar("/home/burhan/Desktop/ClimateAnalysis.jar");
			this.etaClimateJob.setOutputKeyClass(LongWritable.class); 
			this.etaClimateJob.setOutputValueClass(Text.class);
			this.etaClimateJob.setInputFormatClass(CustomFileInputFormat.class); 
			this.etaClimateJob.setOutputFormatClass(TextOutputFormat.class);
			this.etaClimateJob.setMapperClass(EtaClimateMapper.class);
			this.etaClimateJob.setReducerClass(EtaClimateReducer.class);
			this.etaClimateJob.getConfiguration().set("forecastDateOne", ClimateAnalysisDriver.forecastDateOne);
			this.etaClimateJob.getConfiguration().set("forecastDateTwo", ClimateAnalysisDriver.forecastDateTwo);
			for(EtaDirPathsEnum dirPath : EtaDirPathsEnum.values()){
		         MultipleInputs.addInputPath(this.etaClimateJob, dirPath.getPath() , CustomFileInputFormat.class);
			}
		    FileOutputFormat.setOutputPath(this.etaClimateJob, etaClimateOutput);

		    try {
			  return  this.etaClimateJob.waitForCompletion(true)? 1 : 0;
			} catch (ClassNotFoundException | IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
//		    log.info("Creating configuration for marksim climate job");
//		    ClimateAnalysisDriver.marksimConf = new Configuration();
//		    Path marksimClimateOutput = new Path("hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/");
//
//		    this.marksimClimateJob = null;
//			try {
//				this.marksimClimateJob = new Job(ClimateAnalysisDriver.marksimConf, "marksimClimate");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			ClimateAnalysisDriver.marksimConf.set("fs.hdfs.impl", 
//			          org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			  
//			ClimateAnalysisDriver.marksimConf.set("fs.file.impl",
//		            org.apache.hadoop.fs.LocalFileSystem.class.getName());
//			ClimateAnalysisDriver.marksimConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/core-site.xml"));
//			ClimateAnalysisDriver.marksimConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));
//			this.marksimClimateJob.setJarByClass(ClimateAnalysisDriver.class);
//			this.marksimClimateJob.setJar("/home/burhan/Desktop/ClimateAnalysis.jar");
//		    this.marksimClimateJob.setOutputKeyClass(LongWritable.class); 
//		    this.marksimClimateJob.setOutputValueClass(Text.class); 
//		    this.marksimClimateJob.setInputFormatClass(CustomFileInputFormat.class); 
//		    this.marksimClimateJob.setOutputFormatClass(TextOutputFormat.class);
//		    this.marksimClimateJob.setMapperClass( MarksimClimateMapper.class);
//		    this.marksimClimateJob.setReducerClass(MarksimClimateReducer.class);
//		    this.marksimClimateJob.getConfiguration().set("forecastDateOne", ClimateAnalysisDriver.forecastDateOne);
//		    this.marksimClimateJob.getConfiguration().set("forecastDateTwo", ClimateAnalysisDriver.forecastDateTwo);
//			for(MarksimDirPathsEnum dirPath : MarksimDirPathsEnum.values()){
//		         MultipleInputs.addInputPath(this.marksimClimateJob, dirPath.getPath() , CustomFileInputFormat.class);
//			}
//			FileOutputFormat.setOutputPath(this.marksimClimateJob, marksimClimateOutput);
//		    try {
//			  this.marksimClimateJob.waitForCompletion(true);
//			} catch (ClassNotFoundException | IOException | InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		   
//		    log.info("Creating configuration for precis_andes job");
//		    ClimateAnalysisDriver.precisAndesConf = new Configuration();
//		    Path precisAndesClimateOutput = new Path("hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/");
//		    this.precisAndesClimateJob = null;
//			try {
//				this.precisAndesClimateJob = new Job(ClimateAnalysisDriver.precisAndesConf, "precisAndes");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			ClimateAnalysisDriver.precisAndesConf.set("fs.hdfs.impl", 
//			          org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			  
//			ClimateAnalysisDriver.precisAndesConf.set("fs.file.impl",
//		            org.apache.hadoop.fs.LocalFileSystem.class.getName());
//			ClimateAnalysisDriver.precisAndesConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/core-site.xml"));
//			ClimateAnalysisDriver.precisAndesConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));
//			this.precisAndesClimateJob.setJarByClass(ClimateAnalysisDriver.class);
//			this.precisAndesClimateJob.setJar("/home/burhan/Desktop/ClimateAnalysis.jar");
//		    this.precisAndesClimateJob.setOutputKeyClass(LongWritable.class); 
//		    this.precisAndesClimateJob.setOutputValueClass(Text.class); 
//		    this.precisAndesClimateJob.setInputFormatClass(CustomFileInputFormat.class); 
//		    this.precisAndesClimateJob.setMapperClass(PrecisAndesClimateMapper.class);
//		    this.precisAndesClimateJob.setReducerClass(PrecisAndesClimateReducer.class);
//		    this.precisAndesClimateJob.setOutputFormatClass(TextOutputFormat.class);
//		    this.precisAndesClimateJob.getConfiguration().set("forecastDateOne", ClimateAnalysisDriver.forecastDateOne);
//		    this.precisAndesClimateJob.getConfiguration().set("forecastDateTwo", ClimateAnalysisDriver.forecastDateTwo);
//			for(PrecisAndesDirPathsEnum dirPath : PrecisAndesDirPathsEnum.values()){
//		         MultipleInputs.addInputPath(this.precisAndesClimateJob, dirPath.getPath() , CustomFileInputFormat.class);
//			}
//			FileOutputFormat.setOutputPath(this.precisAndesClimateJob, precisAndesClimateOutput);
//		   
//				try {
//					return this.precisAndesClimateJob.waitForCompletion(true) ? 1 : 0;
//				} catch (ClassNotFoundException | InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				return 0;
	    
	    // delete any output that might exist from a previous run of this job
//	    if (fs.exists(FileOutputFormat.getOutputPath(precisAndesClimateJob))) {
//	      fs.delete(FileOutputFormat.getOutputPath(precisAndesClimateJob), true);
//	    }
//
//	    JobControl control = new JobControl("ClimateAnalysisDriver");
//	    ControlledJob etaClimateControlledJob = new ControlledJob(this.etaClimateJob, null);
//	    ControlledJob marksimClimateControlledJob = new ControlledJob(this.marksimClimateJob, null);
//	    ControlledJob precisAndesClimateControlledJob = new ControlledJob(this.precisAndesClimateJob, null);
//	    control.addJob(etaClimateControlledJob);
//	    control.addJob( marksimClimateControlledJob);
//	    control.addJob(precisAndesClimateControlledJob);
//
//	    // execute the jobs
//	    try {
//	      Thread jobControlThread = new Thread(control, "jobcontrol");
//	      jobControlThread.start();
//	      while (!control.allFinished()) {
//	        Thread.sleep(1000);
//	      }
//	      if (control.getFailedJobList().size() > 0) {
//	        throw new IOException("One or more jobs failed");
//	      }
//	    } catch (InterruptedException e) {
//	      throw new IOException("Interrupted while waiting for job control to finish", e);
//	    }
	  }

	  /**
	   * Set the Configuration used by this Program.
	   * 
	   * @param conf The new Configuration to use by this program.
	   */
	  public void setConf(Configuration conf) {
	    ClimateAnalysisDriver.conf = conf; 
	    
	  }

	  /**
	   * Gets the Configuration used by this program.
	   * 
	   * @return This program's Configuration.
	   */
	  public Configuration getConf() {
	    return conf;
	  }

	  /**
	   * @param args Command line arguments.
	   * @throws IOException If an error occurs running the program.
	   */
	  public static void main(String[] args) throws Exception {
		  ClimateAnalysisDriver.conf = new Configuration();

		  ClimateAnalysisDriver.conf.set("fs.hdfs.impl", 
		          org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		  );
		   ClimateAnalysisDriver.conf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName()
		        );
		  ClimateAnalysisDriver.conf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/core-site.xml"));
		  ClimateAnalysisDriver.conf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));
		  LocalDate localDate = LocalDate.now();//For reference
		  DateTimeFormatter formatter = DateTimeFormatter.ofPattern("2017 01 04");
		  String formattedString = localDate.format(formatter);
		  DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("2017 01 04");
		  String formattedString2 = localDate.format(formatter2);
		  forecastDateOne = new String(formattedString);
		  forecastDateTwo = new String(formattedString2);
		  if (args.length < 2) {
	          log.info("Args: <input directory> <output directory>");
	          return;
	      }
	    int res = ToolRunner.run(new ClimateAnalysisDriver(), args);
        System.exit(res);
	  }

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		start(null,null,null,null);
		return 0;
	}
}