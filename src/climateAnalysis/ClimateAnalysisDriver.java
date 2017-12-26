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
import java.util.Random;

import LSTM.LSTM;
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
	  public static class EtaClimateMapperOriginal extends Mapper<LongWritable, Text, Text, LongWritable> {
		  @Override
		  protected void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		  }
      }
	  public static class MarksimClimateMapperOriginal  extends Mapper<LongWritable, Text, Text, LongWritable>{
         @Override
		 protected void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		 }
	  }
	  public static class PrecisAndesClimateMapperOriginal extends Mapper<LongWritable, Text, Text, LongWritable> {
		  @Override
		  protected void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {
		  }
	  }
	  public static class EtaClimateReducerOriginal extends Reducer<Text,LongWritable, Text, LongWritable> {

		  protected void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
		      throws IOException, InterruptedException {

		  }
      }
	  public static class MarksimClimateReducerOriginal  extends Reducer<LongWritable, Text, Text, LongWritable>{

		 protected void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
		      throws IOException, InterruptedException {
		 }
	  }
	  public static class PrecisAndesClimateReducerOriginal extends Reducer<LongWritable, Text, Text, LongWritable> {

		  protected void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
		      throws IOException, InterruptedException {
		  }
	  }
	  public class EtaClimateReducer {

			Configuration configuration = new Configuration();
			public String temp = new String();
			public String fileName = new String();
			final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
			public  LocalDate forecastDateOneLocalDate, forecastDateTwoLocalDate;
			public  LocalDate then = LocalDate.of( 1970, 1, 1 );
			public DataPool dataPool = new DataPool();
			public DataPool  finalDataPool = new DataPool();
			public int tempNcols, tempNrows, tempNoDataValue;
			public double tempXllCorner, tempYllCorner,tempCellSize;
			public int counter;
			public int counterFinalValue = 0;
			public Path tempDirPath = new Path("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-eta-eta_south_america-baseline-1970s/");
			public ArrayList<DataPool> dataPoolArray = new ArrayList<DataPool>();
			private List<Path> dirPaths = new LinkedList<>();

			public void initializePaths(){
				for(EtaDirPathsEnum dirPath : EtaDirPathsEnum.values())
					dirPaths.add(dirPath.getPath());
			}
			public void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
					throws IOException {
				initializePaths();
				final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("d-MMM-yyyy");
				forecastDateOneLocalDate= LocalDate.parse(forecastDateOne, DATE_FORMAT);
				forecastDateTwoLocalDate= LocalDate.parse(forecastDateTwo, DATE_FORMAT);
				BufferedReader br;
				BufferedWriter bw;
				List<Double> valueLine = new ArrayList<Double>();
	            if (dataPoolArray == null)
	            	dataPoolArray = new ArrayList<DataPool>();
		    	if(finalDataPool == null)
		    		finalDataPool = new DataPool();
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
				    	String line;
					    for (FileStatus child : fileStatus) {
					    	fileName = child.getPath().getName();
					    	System.out.println(child.getPath());
					        br=new BufferedReader(new InputStreamReader(fs.open(child.getPath())));
					        counter=0;
					        line=br.readLine();
					        while (line != null){
					        	// value is space-separated values:
					        	// ncols   xxx
					        	// nrows   xxx
					        	// xllCorner xxx
					        	// yllcenter xxx
					        	// cellsize xxx
					        	// nodata_value xxx
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
					        	// xxx xxx xxx xxx xxx xxx xxx xxx xxx xxx
					            // we project out (location, occurrences) so we can iterate through all values for a given cell
					            String[] split = line.split(" ");

					            if (split.length == 2) {
					            	if(split[0].equalsIgnoreCase("ncols")){
					            		dataPool.ncols = Integer.parseInt(split[1]);
					            		counter++;

					                }else if (split[0].equalsIgnoreCase("nrows")){
					                	dataPool.nrows = Integer.parseInt(split[1]);
					                	counter++;

					                }else if (split[0].equalsIgnoreCase("xllCorner")){

					                	dataPool.xllCorner = Double.parseDouble(split[1]);
					                	counter++;

					                }else if (split[0].equalsIgnoreCase("yllCorner")){
					                	dataPool.yllCorner = Double.parseDouble(split[1]);
					                	counter++;

					                }else if (split[0].equalsIgnoreCase("cellsize")){

					                	dataPool.cellsize = Double.parseDouble(split[1]);
					                	counter++;

					                }else if (split[0].equalsIgnoreCase("nodata_value")){
					                	dataPool.nodata_value = Integer.parseInt(split[1]);
					                	counter++;

					                }
					             }else if(split.length > 2) {

					                	for (int x=0; x < dataPool.ncols ; x++){

					                		valueLine.add(new Double (Double.parseDouble(split[x])));
					                	}
					                	Double[] temp = valueLine.toArray(new Double[valueLine.size()]);
					                	dataPool.record.add(temp);
					                	counter++;

					             }
					            line = br.readLine();
					         }

					        	    tempNcols = dataPool.ncols;
					        	    tempNrows = dataPool.nrows;
					        	    tempXllCorner = dataPool.xllCorner;
					        	    tempYllCorner = dataPool.yllCorner;
					        	    tempCellSize = dataPool.cellsize;
					        	    tempNoDataValue = dataPool.nodata_value;
					            	dataPoolArray.add(dataPool);
					            	dataPool.clear();
					            	br.close();
					            	counter = 0;
					       }
					       System.out.println(String.valueOf(tempNcols)+" "+String.valueOf(tempNrows)+ " "+ String.valueOf(tempXllCorner) +
					    		" " + String.valueOf(tempYllCorner)+ " " + String.valueOf(tempCellSize) + " " + String.valueOf(tempNoDataValue));
							// TODO Auto-generated method stub

					    	Period period = Period.between( then, forecastDateOneLocalDate );
					    	int daysInTotal = period.getDays();
					    	int samplePeriodsInTotal = daysInTotal * 72;
						    double[] xData = {0.0, 20.0, 40.0, 60.0, 80.0, 100.0, 120.0, 140.0, 160.0, 180.0, 200.0, 220.0, 240.0};
						    double[] extractedValueList = new double[1220];
						    int i = 0;
						    double[] result = new double[1220];
						    Double[] resultAdditive = new Double[dataPoolArray.get(0).ncols];
						    LSTM lstmHiddenLayer_1 = new LSTM(new Random(), 1,1, 1220);
						    LSTM lstmHiddenLayer_2 = new LSTM(new Random(), 1,1, 1220);
						    LSTM lstmHiddenLayer_3 = new LSTM(new Random(), 1,1, 1220);
						    LSTM lstmHiddenLayer_4 = new LSTM(new Random(), 1,1, 1220);
					    	LSTM lstmTemp = new LSTM(new Random(), 1,1, 1220);
						    lstmHiddenLayer_1.Reset();
						    lstmHiddenLayer_2.Reset();
						    lstmHiddenLayer_3.Reset();
						    lstmHiddenLayer_4.Reset();
						    LSTM[] lstmLayers = new LSTM[4];
						    lstmLayers[0] = lstmHiddenLayer_1;    lstmLayers[1] = lstmHiddenLayer_2;   lstmLayers[2] = lstmHiddenLayer_3;   lstmLayers[3] = lstmHiddenLayer_4;
						    dataPoolArray.get(1).toString();
						    System.out.println("a");
						    for(DataPool dP : dataPoolArray){
						    	for (int j = 0; j < dP.nrows ; j++){
						    		for (int k = 0; k < dP.ncols ; k++){

						    			extractedValueList[i++] = dP.record.get(j)[k].doubleValue();
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
						    					 else if(m-n > 0 && extractedValueList[m-n] != -9999){
						    						 extractedValueList[m] = extractedValueList[m-n];
						    						 FOUND = true;
						    					 }
						    				 if(!FOUND) extractedValueList[m] = lastSelectedValue;
						    			}else{
						    				lastSelectedValue = extractedValueList[m];
						    			}
						    		}
						       }
						    	lstmTemp = lstmLayers[0];
						    	for ( i =1; i < lstmLayers.length; i++) {
						    		try {
										 result = lstmLayers[i].Next(lstmTemp.Next(extractedValueList));
									} catch (Exception e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
						    		lstmLayers[i].SetParameters(lstmTemp.GetParameters());
						    		lstmTemp = lstmLayers[i];
						    	}

							    finalDataPool.ncols =  tempNcols;
							    finalDataPool.nrows =  tempNrows;
							    finalDataPool.xllCorner = tempXllCorner;
							    finalDataPool.yllCorner = tempYllCorner;
							    finalDataPool.cellsize =  tempCellSize;
							    finalDataPool.nodata_value = tempNoDataValue;
							    for( int j=0; j < dP.nrows; j++) {
							    	for(int k=0; k < dP.ncols; k++) {
							    		resultAdditive[k] = new Double(result[j + k]);
							    	}
							    	finalDataPool.record.add(resultAdditive);
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
							temp += "/" + fileName.replaceAll("[0-9]+","") + "-Predicted-for-" + forecastDateOne.toString();
							Path absoluteFilePath = new Path(temp);
						    FSDataOutputStream os = hdfs.create( absoluteFilePath);
						    bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
						    bw.write("ncols " + finalDataPool.ncols);
						    bw.newLine();
						    bw.write("nrows " + finalDataPool.nrows);
						    bw.newLine();
						    bw.write("xllCorner " + finalDataPool.xllCorner);
						    bw.newLine();
						    bw.write("yllCorner " + finalDataPool.yllCorner);
						    bw.newLine();
						    bw.write("nodata_value " + finalDataPool.nodata_value);
						    bw.newLine();
						    for(int k=0; k < finalDataPool.nrows + 6 ; k++){
						    	for(int m = 0; m < finalDataPool.ncols; m++){
						    		bw.write( String.valueOf(finalDataPool.record.get(k)[m]));
						    	}
						    	bw.newLine();
						    }
						    bw.close();
						    hdfs.close();
//						    try {
//								context.write(arg0, (LongWritable) arg1);
//							} catch (InterruptedException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
						    dataPool.clear();
						    finalDataPool.clear();
						    dataPoolArray.clear();

				    } else {
							    // Handle the case where dir is not really a directory.
							    // Checking dir.isDirectory() above would not be sufficient
							    // to avoid race conditions with another process that deletes
							    // directories.
				    }
				}
			}
		}

	  public class MarksimClimateReducer {

	  	Configuration configuration = new Configuration();
	  	public String temp = new String();
	  	public String fileName = new String();
	  	final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
	  	public  LocalDate forecastDateOneLocalDate, forecastDateTwoLocalDate;
	  	public  LocalDate then = LocalDate.of( 2000, 1, 1 );
	  	public DataPool dataPool = new DataPool();
	  	public DataPool finalDataPool = new DataPool();
		public int tempNcols, tempNrows, tempNoDataValue;
		public double tempXllCorner, tempYllCorner,tempCellSize;
	  	public  int counter;
	  	public  int counterFinalValue = 0;
	  	public Path tempDirPath = new Path("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/");
	  	public ArrayList<DataPool> dataPoolArray = new ArrayList<DataPool>();
	  	private List<Path> dirPaths = new LinkedList<>();

	  	public void initializePaths(){
			for(MarksimDirPathsEnum dirPath : MarksimDirPathsEnum.values())
				dirPaths.add(dirPath.getPath());
	  	}
	  	public void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
	  			throws IOException {
	  		initializePaths();
	  		final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("d-MMM-yyyy");
	  		forecastDateOneLocalDate= LocalDate.parse(forecastDateOne, DATE_FORMAT);
	  		forecastDateTwoLocalDate= LocalDate.parse(forecastDateTwo, DATE_FORMAT);
	  		BufferedReader br;
	  		BufferedWriter bw;
			List<Double> valueLine = new ArrayList<Double>();
	        if (dataPoolArray == null)
	        	dataPoolArray = new ArrayList<DataPool>();
	    	if(finalDataPool == null)
	    		finalDataPool = new DataPool();
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
	  		    	String line;
	  			    for (FileStatus child : fileStatus) {
	  			    	fileName = child.getPath().getName();
	  			    	System.out.println( fileName);
	  			        br=new BufferedReader(new InputStreamReader(fs.open(child.getPath())));
	  			        counter = 0;
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

	  			            if (split.length == 2) {
	  			            	if(split[0].equalsIgnoreCase("ncols")){
	  			            		dataPool.ncols = Integer.parseInt(split[1]);
	  			            		counter++;

	  			                }else if (split[0].equalsIgnoreCase("nrows")){
	  			                	dataPool.nrows = Integer.parseInt(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("xllCorner")){
	  			                	dataPool.xllCorner = Double.parseDouble(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("yllCorner")){
	  			                	dataPool.yllCorner = Double.parseDouble(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("cellsize")){
	  			                	dataPool.cellsize = Double.parseDouble(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("nodata_value")){
	  			                	dataPool.nodata_value = Integer.parseInt(split[1]);
	  			                	counter++;

	  			                }
	  			            }else if(split.length > 2) {
	  			                	for (int x=0; x < dataPool.ncols ; x++){
				                		valueLine.add(new Double (Double.parseDouble(split[x])));
	  			                	}
				                	Double[] temp = valueLine.toArray(new Double[valueLine.size()]);
				                	dataPool.record.add(temp);
	  			                    counter++;
	  			            }
	  			            line = br.readLine();
	  			        }
			        	    tempNcols = dataPool.ncols;
			        	    tempNrows = dataPool.nrows;
			        	    tempXllCorner = dataPool.xllCorner;
			        	    tempYllCorner = dataPool.yllCorner;
			        	    tempCellSize = dataPool.cellsize;
			        	    tempNoDataValue = dataPool.nodata_value;
	  			        	 dataPoolArray.add(dataPool);
	  			          	 dataPool.clear();
	  			          	 br.close();
	  			             counter = 0;

	  			    }
	  			    	Period period = Period.between( then, forecastDateOneLocalDate );
	  			    	int daysInTotal = period.getDays();
	  			    	int samplePeriodsInTotal = daysInTotal * 288;
	  				    double[] extractedValueList = new double[50];
	  				    int i = 0;
	  				    double[] result = new double[1220];
					    Double[] resultAdditive = new Double[dataPoolArray.get(0).ncols];
					    LSTM lstmHiddenLayer_1 = new LSTM(new Random(), 1,1, 1220);
					    LSTM lstmHiddenLayer_2 = new LSTM(new Random(), 1,1, 1220);
					    LSTM lstmHiddenLayer_3 = new LSTM(new Random(), 1,1, 1220);
					    LSTM lstmHiddenLayer_4 = new LSTM(new Random(), 1,1, 1220);
				    	LSTM lstmTemp = new LSTM(new Random(), 1,1, 1220);
					    lstmHiddenLayer_1.Reset();
					    lstmHiddenLayer_2.Reset();
					    lstmHiddenLayer_3.Reset();
					    lstmHiddenLayer_4.Reset();
					    LSTM[] lstmLayers = new LSTM[4];
					    lstmLayers[0] = lstmHiddenLayer_1;    lstmLayers[1] = lstmHiddenLayer_2;   lstmLayers[2] = lstmHiddenLayer_3;   lstmLayers[3] = lstmHiddenLayer_4;
					    //dataPoolArray.get(1).toString();

					    for(DataPool dP : dataPoolArray){
					    	for (int j = 0; j < dP.nrows ; j++){
					    		for (int k = 0; k < dP.ncols ; k++){

					    			extractedValueList[i++] = dP.record.get(j)[k].doubleValue();
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
					    					 else if(m-n > 0 && extractedValueList[m-n] != -9999){
					    						 extractedValueList[m] = extractedValueList[m-n];
					    						 FOUND = true;
					    					 }
					    				 if(!FOUND) extractedValueList[m] = lastSelectedValue;
					    			}else{
					    				lastSelectedValue = extractedValueList[m];
					    			}
					    		}
					       }
					    	lstmTemp = lstmLayers[0];
					    	for ( i =1; i < lstmLayers.length; i++) {
					    		try {
									 result = lstmLayers[i].Next(lstmTemp.Next(extractedValueList));
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
					    		lstmLayers[i].SetParameters(lstmTemp.GetParameters());
					    		lstmTemp = lstmLayers[i];
					    	}

						    finalDataPool.ncols =  tempNcols;
						    finalDataPool.nrows =  tempNrows;
						    finalDataPool.xllCorner = tempXllCorner;
						    finalDataPool.yllCorner = tempYllCorner;
						    finalDataPool.cellsize =  tempCellSize;
						    finalDataPool.nodata_value = tempNoDataValue;
						    for( int j=0; j < dP.nrows; j++) {
						    	for(int k=0; k < dP.ncols; k++) {
						    		resultAdditive[k] = new Double(result[j + k]);
						    	}
						    	finalDataPool.record.add(resultAdditive);
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
	  					temp += "/" + fileName.replaceAll("[0-9]+","") + "-Predicted-for-" + forecastDateOne.toString();
	  					Path absoluteFilePath = new Path(temp);
	  				    FSDataOutputStream os = hdfs.create( absoluteFilePath);
	  				    bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
	  				    bw.write("ncols " + finalDataPool.ncols);
	  				    bw.newLine();
	  				    bw.write("nrows " + finalDataPool.nrows);
	  				    bw.newLine();
	  				    bw.write("xllCorner " + finalDataPool.xllCorner);
	  				    bw.newLine();
	  				    bw.write("yllCorner " + finalDataPool.yllCorner);
	  				    bw.newLine();
	  				    bw.write("nodata_value " + finalDataPool.nodata_value);
	  				    bw.newLine();
	  				    for(int k=0; k < finalDataPool.nrows + 6 ; k++){
	  				    	for(int m = 0; m < finalDataPool.ncols; m++){
	  				    		bw.write( String.valueOf(finalDataPool.record.get(k)[m]));
	  				    	}
	  				    	bw.newLine();
	  				    }
	  				   tempNcols = 0; tempNrows = 0; tempXllCorner = 0.0; tempYllCorner = 0.0; tempCellSize = 0.0; tempNoDataValue = 0;
	  				    bw.close();
	  				    hdfs.close();
//	  				    try {
//	  						context.write(arg0, (LongWritable) arg1);
//	  					} catch (InterruptedException e) {
//	  						// TODO Auto-generated catch block
//	  						e.printStackTrace();
//	  					}
					     dataPool.clear();
					     finalDataPool.clear();
					     dataPoolArray.clear();

	  		    } else {
	  					    // Handle the case where dir is not really a directory.
	  					    // Checking dir.isDirectory() above would not be sufficient
	  					    // to avoid race conditions with another process that deletes
	  					    // directories.
	  		    }
	  		}
	  	}
	  }

	  public class PrecisAndesClimateReducer {

	  	Configuration configuration = new Configuration();
	  	public String temp = new String();
	  	public String fileName = new String();
	  	final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
	  	public  LocalDate forecastDateOneLocalDate, forecastDateTwoLocalDate;
	  	public LocalDate then1 = LocalDate.of( 1970, 1, 1 );
	  	public LocalDate then2 = LocalDate.of( 1990, 1, 1 );
	  	public DataPool dataPool = new DataPool();
	  	public DataPool finalDataPool = new DataPool();
		public int tempNcols, tempNrows, tempNoDataValue;
		public double tempXllCorner, tempYllCorner,tempCellSize;
	  	public int counter;
	  	public int counterFinalValue = 0;
	  	public Path tempDirPath = new Path("hdfs://localhost:9000/ClimateData/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/");
	  	public ArrayList<DataPool> dataPoolArray = new ArrayList<DataPool>();



	  	private List<Path> dirPaths = new LinkedList<>();

	  	public void initializePaths(){
			for(PrecisAndesDirPathsEnum dirPath : PrecisAndesDirPathsEnum.values())
				dirPaths.add(dirPath.getPath());
	  	}
	  	public void reduce(Text arg0, Iterator<LongWritable> arg1, Context context)
	  			throws IOException {
	  		initializePaths();
	  		final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("d-MMM-yyyy");
	  		forecastDateOneLocalDate= LocalDate.parse(forecastDateOne, DATE_FORMAT);
	  		forecastDateTwoLocalDate= LocalDate.parse(forecastDateTwo, DATE_FORMAT);
	  		BufferedReader br;
	  		BufferedWriter bw;
			List<Double> valueLine = new ArrayList<Double>();
	        if (dataPoolArray == null)
	        	dataPoolArray = new ArrayList<DataPool>();
	    	if(finalDataPool == null)
	    		finalDataPool = new DataPool();
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
	  		    	String line;
	  			    for (FileStatus child : fileStatus) {
	  			    	fileName = child.getPath().getName();
	  			    	System.out.println(fileName);
	  			        br=new BufferedReader(new InputStreamReader(fs.open(child.getPath())));
	  			        counter = 0;
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

	  			            if (split.length == 2) {
	  			            	if(split[0].equalsIgnoreCase("ncols")){
	  			            		dataPool.ncols = Integer.parseInt(split[1]);
	  			            		counter++;

	  			                }else if (split[0].equalsIgnoreCase("nrows")){
	  			                	dataPool.nrows = Integer.parseInt(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("xllCorner")){
	  			                	dataPool.xllCorner = Double.parseDouble(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("yllCorner")){
	  			                	dataPool.yllCorner = Double.parseDouble(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("cellsize")){
	  			                	dataPool.cellsize = Double.parseDouble(split[1]);
	  			                	counter++;

	  			                }else if (split[0].equalsIgnoreCase("nodata_value")){
	  			                	dataPool.nodata_value = Integer.parseInt(split[1]);
	  			                	counter++;

	  			                }
	  			           }else if(split.length > 2) {

	  			                	for (int x=0; x < dataPool.ncols ; x++){

				                		valueLine.add(new Double (Double.parseDouble(split[x])));

	  			                	}
				                	Double[] temp = valueLine.toArray(new Double[valueLine.size()]);
				                	dataPool.record.add(temp);
	  			                	counter++;
	  			           }
	  			            line=br.readLine();
	  			        }

			        	    tempNcols = dataPool.ncols;
			        	    tempNrows = dataPool.nrows;
			        	    tempXllCorner = dataPool.xllCorner;
			        	    tempYllCorner = dataPool.yllCorner;
			        	    tempCellSize = dataPool.cellsize;
			        	    tempNoDataValue = dataPool.nodata_value;
	  			        	 dataPoolArray.add(dataPool);
	  			           	 dataPool.clear();
	  			           	 br.close();
	  			             counter = 0;

	  			       }
	  			    	Period period = Period.between( then1, forecastDateOneLocalDate );
	  			    	int daysInTotal = period.getDays();
	  			    	int samplePeriodsInTotal = daysInTotal * 72;
	  				    double[] extractedValueList = new double[50];
	  				    int i = 0;
	  				    double[] result = new double[1220];
					    Double[] resultAdditive = new Double[dataPoolArray.get(0).ncols];
					    LSTM lstmHiddenLayer_1 = new LSTM(new Random(), 1,1, 1220);
					    LSTM lstmHiddenLayer_2 = new LSTM(new Random(), 1,1, 1220);
					    LSTM lstmHiddenLayer_3 = new LSTM(new Random(), 1,1, 1220);
					    LSTM lstmHiddenLayer_4 = new LSTM(new Random(), 1,1, 1220);
				    	LSTM lstmTemp = new LSTM(new Random(), 1,1, 1220);
					    lstmHiddenLayer_1.Reset();
					    lstmHiddenLayer_2.Reset();
					    lstmHiddenLayer_3.Reset();
					    lstmHiddenLayer_4.Reset();
					    LSTM[] lstmLayers = new LSTM[4];
					    lstmLayers[0] = lstmHiddenLayer_1;    lstmLayers[1] = lstmHiddenLayer_2;   lstmLayers[2] = lstmHiddenLayer_3;   lstmLayers[3] = lstmHiddenLayer_4;
					    //dataPoolArray.get(1).toString();

					    for(DataPool dP : dataPoolArray){
					    	for (int j = 0; j < dP.nrows ; j++){
					    		for (int k = 0; k < dP.ncols ; k++){

					    			extractedValueList[i++] = dP.record.get(j)[k].doubleValue();
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
					    					 else if(m-n > 0 && extractedValueList[m-n] != -9999){
					    						 extractedValueList[m] = extractedValueList[m-n];
					    						 FOUND = true;
					    					 }
					    				 if(!FOUND) extractedValueList[m] = lastSelectedValue;
					    			}else{
					    				lastSelectedValue = extractedValueList[m];
					    			}
					    		}
					       }
					    	lstmTemp = lstmLayers[0];
					    	for ( i =1; i < lstmLayers.length; i++) {
					    		try {
									 result = lstmLayers[i].Next(lstmTemp.Next(extractedValueList));
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
					    		lstmLayers[i].SetParameters(lstmTemp.GetParameters());
					    		lstmTemp = lstmLayers[i];
					    	}

						    finalDataPool.ncols =  tempNcols;
						    finalDataPool.nrows =  tempNrows;
						    finalDataPool.xllCorner = tempXllCorner;
						    finalDataPool.yllCorner = tempYllCorner;
						    finalDataPool.cellsize =  tempCellSize;
						    finalDataPool.nodata_value = tempNoDataValue;
						    for( int j=0; j < dP.nrows; j++) {
						    	for(int k=0; k < dP.ncols; k++) {
						    		resultAdditive[k] = new Double(result[j + k]);
						    	}
						    	finalDataPool.record.add(resultAdditive);
						    }
					    }

	  				    FileSystem hdfs = null;
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
	  					temp += "/" + fileName.replaceAll("[0-9]+","") + "-Predicted-for-" + forecastDateOne.toString();
	  					Path absoluteFilePath = new Path(temp);
	  				    FSDataOutputStream os = hdfs.create( absoluteFilePath);
	  				    bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
	  				    bw.write("ncols " + finalDataPool.ncols);
	  				    bw.newLine();
	  				    bw.write("nrows " + finalDataPool.nrows);
	  				    bw.newLine();
	  				    bw.write("xllCorner " + finalDataPool.xllCorner);
	  				    bw.newLine();
	  				    bw.write("yllCorner " + finalDataPool.yllCorner);
	  				    bw.newLine();
	  				    bw.write("nodata_value " + finalDataPool.nodata_value);
	  				    bw.newLine();
	  				    for(int k=0; k < finalDataPool.nrows + 6 ; k++){
	  				    	for(int m = 0; m < finalDataPool.ncols; m++){
	  				    		bw.write( String.valueOf(finalDataPool.record.get(k)[m]));
	  				    	}
	  				    	bw.newLine();
	  				    }
	  				   tempNcols = 0; tempNrows = 0; tempXllCorner = 0.0; tempYllCorner = 0.0; tempCellSize = 0.0; tempNoDataValue = 0;
	  				    bw.close();
	  				    hdfs.close();
//	  				    try {
//	  						context.write(arg0, (LongWritable) arg1);
//	  					} catch (InterruptedException e) {
//	  						// TODO Auto-generated catch block
//	  						e.printStackTrace();
//	  					}
					     dataPool.clear();
					     finalDataPool.clear();
					     dataPoolArray.clear();

	  		    } else {
	  					    // Handle the case where dir is not really a directory.
	  					    // Checking dir.isDirectory() above would not be sufficient
	  					    // to avoid race conditions with another process that deletes
	  					    // directories.
	  		    }
	  		}
	  	}
	  }

	  @SuppressWarnings({"deprecation" })
	public int startOne(Path[] etaInputDir, Path[] precisAndesInputDir, Path marksimInputDir, Path outputDir) throws IOException {

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
			this.etaClimateJob.setMapperClass(EtaClimateMapperOriginal.class);
			this.etaClimateJob.setReducerClass(EtaClimateReducerOriginal.class);
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

		    log.info("Creating configuration for marksim climate job");
		    ClimateAnalysisDriver.marksimConf = new Configuration();
		    Path marksimClimateOutput = new Path("hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-pattern_scaling_marksim-baseline-2000s/");

		    this.marksimClimateJob = null;
			try {
				this.marksimClimateJob = new Job(ClimateAnalysisDriver.marksimConf, "marksimClimate");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ClimateAnalysisDriver.marksimConf.set("fs.hdfs.impl",
			          org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

			ClimateAnalysisDriver.marksimConf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName());
			ClimateAnalysisDriver.marksimConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/core-site.xml"));
			ClimateAnalysisDriver.marksimConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));
			this.marksimClimateJob.setJarByClass(ClimateAnalysisDriver.class);
			this.marksimClimateJob.setJar("/home/burhan/Desktop/ClimateAnalysis.jar");
		    this.marksimClimateJob.setOutputKeyClass(LongWritable.class);
		    this.marksimClimateJob.setOutputValueClass(Text.class);
		    this.marksimClimateJob.setInputFormatClass(CustomFileInputFormat.class);
		    this.marksimClimateJob.setOutputFormatClass(TextOutputFormat.class);
		    this.marksimClimateJob.setMapperClass( MarksimClimateMapperOriginal.class);
		    this.marksimClimateJob.setReducerClass(MarksimClimateReducerOriginal.class);
		    this.marksimClimateJob.getConfiguration().set("forecastDateOne", ClimateAnalysisDriver.forecastDateOne);
		    this.marksimClimateJob.getConfiguration().set("forecastDateTwo", ClimateAnalysisDriver.forecastDateTwo);
			for(MarksimDirPathsEnum dirPath : MarksimDirPathsEnum.values()){
		         MultipleInputs.addInputPath(this.marksimClimateJob, dirPath.getPath() , CustomFileInputFormat.class);
			}
			FileOutputFormat.setOutputPath(this.marksimClimateJob, marksimClimateOutput);
		    try {
			  this.marksimClimateJob.waitForCompletion(true);
			} catch (ClassNotFoundException | IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		    log.info("Creating configuration for precis_andes job");
		    ClimateAnalysisDriver.precisAndesConf = new Configuration();
		    Path precisAndesClimateOutput = new Path("hdfs://localhost:9000/ClimateDataForecast/cgiardata-ccafs-ccafs-climate-data-precis-precis_andes-baseline-1970s/");
		    this.precisAndesClimateJob = null;
			try {
				this.precisAndesClimateJob = new Job(ClimateAnalysisDriver.precisAndesConf, "precisAndes");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ClimateAnalysisDriver.precisAndesConf.set("fs.hdfs.impl",
			          org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

			ClimateAnalysisDriver.precisAndesConf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName());
			ClimateAnalysisDriver.precisAndesConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/core-site.xml"));
			ClimateAnalysisDriver.precisAndesConf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));
			this.precisAndesClimateJob.setJarByClass(ClimateAnalysisDriver.class);
			this.precisAndesClimateJob.setJar("/home/burhan/Desktop/ClimateAnalysis.jar");
		    this.precisAndesClimateJob.setOutputKeyClass(LongWritable.class);
		    this.precisAndesClimateJob.setOutputValueClass(Text.class);
		    this.precisAndesClimateJob.setInputFormatClass(CustomFileInputFormat.class);
		    this.precisAndesClimateJob.setMapperClass(PrecisAndesClimateMapperOriginal.class);
		    this.precisAndesClimateJob.setReducerClass(PrecisAndesClimateReducerOriginal.class);
		    this.precisAndesClimateJob.setOutputFormatClass(TextOutputFormat.class);
		    this.precisAndesClimateJob.getConfiguration().set("forecastDateOne", ClimateAnalysisDriver.forecastDateOne);
		    this.precisAndesClimateJob.getConfiguration().set("forecastDateTwo", ClimateAnalysisDriver.forecastDateTwo);
			for(PrecisAndesDirPathsEnum dirPath : PrecisAndesDirPathsEnum.values()){
		         MultipleInputs.addInputPath(this.precisAndesClimateJob, dirPath.getPath() , CustomFileInputFormat.class);
			}
			FileOutputFormat.setOutputPath(this.precisAndesClimateJob, precisAndesClimateOutput);

				try {
					return this.precisAndesClimateJob.waitForCompletion(true) ? 1 : 0;
				} catch (ClassNotFoundException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return 0;


	  }
	  public int startTwo(Path[] etaInputDir, Path[] precisAndesInputDir, Path marksimInputDir, Path outputDir) throws IOException {

          EtaClimateReducer etaClimateReducer = new EtaClimateReducer();
          etaClimateReducer.reduce(null, null, null);
          MarksimClimateReducer marksimClimateReducer = new MarksimClimateReducer();
          //marksimClimateReducer.reduce(null, null, null);
          PrecisAndesClimateReducer precisAndesClimateReducer = new PrecisAndesClimateReducer();
          //precisAndesClimateReducer.reduce(null, null, null);

		  return 1;
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
		  ClimateAnalysisDriver.etaConf = new Configuration();
		  ClimateAnalysisDriver.marksimConf = new Configuration();
		  ClimateAnalysisDriver.precisAndesConf = new Configuration();
		  ClimateAnalysisDriver.conf.set("fs.hdfs.impl",
		          org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		  );
		   ClimateAnalysisDriver.conf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName()
		        );
		  ClimateAnalysisDriver.conf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/core-site.xml"));
		  ClimateAnalysisDriver.conf.addResource(new Path("/$HADOOP_HOME/etc/hadoop/hdfs-site.xml"));

		  forecastDateOne = args[0];
		  forecastDateTwo = args[1];
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
		//startOne(null,null,null,null);
		startTwo(null,null,null,null);
		return 0;
	}
}