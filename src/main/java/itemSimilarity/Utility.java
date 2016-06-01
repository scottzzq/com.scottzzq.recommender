package itemSimilarity;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Utility {
	public static String getDate(int beforeDays) {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Date inputDate = new Date();

		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);

		int inputDayOfYear = cal.get(Calendar.DAY_OF_YEAR);
		cal.set(Calendar.DAY_OF_YEAR, inputDayOfYear - beforeDays);

		return dateFormat.format(cal.getTime());
	}

	public static String getDateFormat(String dateformat, int beforeDays) {
		DateFormat dateFormat = new SimpleDateFormat(dateformat);
		Date inputDate = new Date();

		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);

		int inputDayOfYear = cal.get(Calendar.DAY_OF_YEAR);
		cal.set(Calendar.DAY_OF_YEAR, inputDayOfYear - beforeDays);
		return dateFormat.format(cal.getTime());
	}

	public static String getHour(int beforeHours) {
		DateFormat dateFormat = new SimpleDateFormat("HH");
		Date inputDate = new Date();

		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);

		int inputHour = cal.get(Calendar.HOUR_OF_DAY);
		System.out.println(inputHour);
		int hour = inputHour - beforeHours;
		if (hour < 0) {
			hour += 24;
		}
		cal.set(Calendar.HOUR_OF_DAY, inputHour - beforeHours);
		return dateFormat.format(cal.getTime());
	}

	@SuppressWarnings("deprecation")
	public static String nhourAgo(String start, int n) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		Date dt = sdf.parse(start);
		int year = dt.getYear() + 1900;
		int month = dt.getMonth();
		int day = dt.getDate();
		int hour = dt.getHours();
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month);
		cal.set(Calendar.DAY_OF_MONTH, day);
		cal.set(Calendar.HOUR_OF_DAY, hour);
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH");
		cal.add(Calendar.HOUR, -n);
		String end = sdf1.format(cal.getTime());
		return end;
	}

	public static long getInterval(String time1, String time2) throws Exception {
		String day1 = time1.substring(0, 10);
		String day2 = time2.substring(0, 10);
		String hour1 = time1.substring(11);
		String hour2 = time2.substring(11);
		if (hour2.length() > 8)
			hour2 = hour2.substring(0, 8);
		SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd");
		Date date1 = myFormatter.parse(day1);
		Date date2 = myFormatter.parse(day2);
		long minus_day = (date1.getTime() - date2.getTime());
		// System.out.println(day1+"\t"+day2+"\t"+minus_day);
		String[] t1 = hour1.split(":");
		String[] t2 = hour2.split(":");
		int minus1 = Integer.parseInt(t1[0]) * 3600 + Integer.parseInt(t1[1])
				* 60 + Integer.parseInt(t1[2]);
		int minus2 = Integer.parseInt(t2[0]) * 3600 + Integer.parseInt(t2[1])
				* 60 + Integer.parseInt(t2[2]);
		return (minus_day / 1000 + minus1 - minus2) / (3600);
	}
}
