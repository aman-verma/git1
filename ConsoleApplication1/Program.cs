using System;// For system functions like Console.
using System.Collections.Generic; // For generic collections like List.
using System.Data.SqlClient;// For the database connections and objects.
using System.Configuration;
using System.Data;
using System.Globalization;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {

            Dictionary<string, List<string>> TickRemoveData = new Dictionary<string, List<string>>();
            Dictionary<string, double> TickBaseValue = new Dictionary<string, double>();
            Dictionary<string, List<string>> MinuteRemoveData = new Dictionary<string, List<string>>();
            Dictionary<string, double> MinuteBaseValue = new Dictionary<string, double>();
            DataTable TickDataTable = new DataTable();
            DataTable MinuteDataTable = new DataTable();
            DataTable avgPrice = new DataTable();
            string TickDate = ConfigurationManager.AppSettings["TickDate"];
            var DateToSearchTick = "Tick" + TickDate;
            var DateToSearchMinute = "Minute" + TickDate;
            string connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
            SqlConnection connection = new SqlConnection(connectionString);
            {
                connection.Open();
                SqlCommand commandforTick = new SqlCommand("SELECT * FROM " + DateToSearchTick, connection);
                SqlCommand commandforMinute = new SqlCommand("SELECT * FROM " + DateToSearchMinute, connection);
                //SqlDataReader reader = command.ExecuteReader();

                using (SqlDataAdapter a = new SqlDataAdapter(commandforTick))
                {
                    a.Fill(TickDataTable);
                }
                using (SqlDataAdapter a = new SqlDataAdapter(commandforMinute))
                {
                    a.Fill(MinuteDataTable);
                }

            }
            foreach (DataRow dr in TickDataTable.Rows)
            {
                string value2 = dr["DateTimeStamp"].ToString(); ;
                double value1 = Convert.ToDouble(dr["Price"]);
                string key = dr["CurrencyPair"].ToString();
                if (TickBaseValue.ContainsKey(key))
                {

                    double valueofkey = TickBaseValue[key];
                    double value = (10 * valueofkey) / 100;
                    double valueafteradd = valueofkey + value;
                    double valueaftersubtract = valueofkey - value;
                    if (value1 >= valueafteradd || value1 <= valueaftersubtract)
                    {

                        // TickBaseValue.Remove(key);
                        //TickRemoveKeyValue.Add(key, list<double>value);
                        if (TickRemoveData.ContainsKey(key))
                        {
                            //key = value2.Insert;
                            //TickRemoveData.Add(key, new List<string>());
                            TickRemoveData[key].Add(value2);
                        }
                        else
                        {
                            //  TickRemoveData.Add(key, new List<string>());
                            TickRemoveData.Add(key, new List<string>());
                            TickRemoveData[key].Add(value2);
                        }

                    }
                    else
                    {
                        TickBaseValue[key] = value1;

                    }
                }
                else
                {
                    TickBaseValue[key] = value1;
                }

            }

            foreach (KeyValuePair<string, List<string>> kvp in TickRemoveData)
            {
                string key = kvp.Key;
                //Response.Write("Value " + kvp.Value);
                foreach (String value in kvp.Value)
                {
                    string values = value;

                    SqlCommand deletecommand = new SqlCommand("DELETE FROM " + DateToSearchTick + " WHERE CurrencyPair= '" + key + "' AND DatetimeStamp= '" + values + "' And PriceType= 'Mid'", connection);
                    deletecommand.ExecuteNonQuery();
                }

            }
            foreach (DataRow dr in MinuteDataTable.Rows)
            {
                string valuefordatetime = dr["DateTimeStamp"].ToString(); ;
                double valueforprice = Convert.ToDouble(dr["Price"]);
                string key = dr["CurrencyPair"].ToString();
                if (MinuteBaseValue.ContainsKey(key))
                {

                    double valueofkey = MinuteBaseValue[key];
                    double value = (10 * valueofkey) / 100;
                    double valueafteradd = valueofkey + value;
                    double valueaftersubtract = valueofkey - value;
                    if (valueforprice >= valueafteradd || valueforprice <= valueaftersubtract)
                    {

                        // TickBaseValue.Remove(key);
                        //TickRemoveKeyValue.Add(key, list<double>value);
                        if (MinuteRemoveData.ContainsKey(key))
                        {
                            //key = value2.Insert;
                            //TickRemoveData.Add(key, new List<string>());
                            MinuteRemoveData[key].Add(valuefordatetime);
                        }
                        else
                        {
                            //  TickRemoveData.Add(key, new List<string>());
                            MinuteRemoveData.Add(key, new List<string>());
                            MinuteRemoveData[key].Add(valuefordatetime);
                        }

                    }
                    else
                    {
                        MinuteBaseValue[key] = valueforprice;

                    }
                }
                else
                {
                    MinuteBaseValue[key] = valueforprice;
                }

            }
            foreach (KeyValuePair<string, List<string>> kvp in MinuteRemoveData)
            {
                string key = kvp.Key;
                //Response.Write("Value " + kvp.Value);
                foreach (String value in kvp.Value)
                {
                    string values = value;
                    IFormatProvider culture = new CultureInfo("en-US", true);
                    DateTime dt = DateTime.ParseExact(values, "dd-MM-yyyy HH:mm:ss", culture);

                    SqlCommand selectcommand = new SqlCommand("Select Price FROM " + DateToSearchTick + " WHERE CurrencyPair= '" + key + "' AND DatetimeStamp = (Select Min(DateTimeStamp) from " + DateToSearchTick + " WHERE CurrencyPair= '" + key + "' AND PriceType='Mid' AND DatetimeStamp >='" + dt.ToString("dd-MM-yyyy HH:mm:ss.fff") + "' and DatetimeStamp <= '" + dt.AddMinutes(1).ToString("dd-MM-yyyy HH:mm:ss.fff") + "')  ", connection);
                    selectcommand.ExecuteNonQuery();
                    using (SqlDataReader reader = selectcommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            SqlCommand updatecommand = new SqlCommand("UPDATE " + DateToSearchMinute + "  Set Price = (SELECT TOP 1 Price FROM " + DateToSearchMinute + " WHERE CurrencyPair= '" + key + "' AND DatetimeStamp = '" + dt.AddMinutes(-1).ToString("dd-MM-yyyy HH:mm:ss") + "')  WHERE CurrencyPair= '" + key + "' AND DatetimeStamp ='" + dt.ToString("dd-MM-yyyy HH:mm:ss") + "') ", connection);
                            updatecommand.ExecuteNonQuery();
                          
                        }
                        reader.Close(); 

                    }
                                                                      
                    SqlCommand selectcommandforaggregation = new SqlCommand("SELECT avg(Price)FROM " + DateToSearchTick + " WHERE CurrencyPair= '" + key + "' AND [DatetimeStamp] between  '" + dt.ToString("dd-MM-yyyy" + " 07:00:00") + "' and '" + dt.ToString("dd-MM-yyyy" + "13:00:00") + "'", connection);
                    selectcommandforaggregation.ExecuteNonQuery();
                    using (SqlDataAdapter a = new SqlDataAdapter(selectcommandforaggregation))
                    {
                        a.Fill(avgPrice);
                    }
                    foreach (DataRow dr in MinuteDataTable.Rows)
                    {
                        //string name = row["name"].ToString();

                    int a =b;
                    }
                    //SqlCommand updatecommandfortwap = new SqlCommand("UPDATE TWAP" + dt.ToString("MMyyyy") + " Set Price WHERE CurrencyPair= '" + key + "' AND DatetimeStamp = '" + dt.ToString("MMM-yyyy") + "'", connection);
                    //updatecommandfortwap.ExecuteNonQuery();
                    //foreach (DataRow dr in dt.Rows)
                    //{
                    //    PrepareDocument(dr);
                    //}
                    using (SqlDataReader reader = selectcommandforaggregation.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            SqlCommand updatecommandfortwap = new SqlCommand("UPDATE TWAP" + dt.ToString("MMyyyy") + " Set Price WHERE CurrencyPair= '" + key + "' AND DatetimeStamp = '" + dt.ToString("MMM-yyyy") + "'", connection);
                            updatecommandfortwap.ExecuteNonQuery();
                        }
                        reader.Close();

                   }
                   
                }

            }

            
            connection.Close();
        }


    }
}


