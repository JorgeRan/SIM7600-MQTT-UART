#include <bits/stdc++.h>
#include <boost/asio.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "../arduPi.h"
#include "../sim7x00.h"
#include "MQTT_config.h"



using boost::asio::ip::udp;
using namespace std;

static const int POWERKEY = 6;
static const char APN[]         = "ltemobile.apn";   



std::string getDroneName() {
    std::ifstream file("/etc/hostapd/hostapd.conf");
    if (!file.is_open()) {
        return "";
    }

    std::string line;
    std::string ssid;
    while (std::getline(file, line)) {
        if (line.rfind("ssid=", 0) == 0) {
            ssid = line.substr(5);
            break;
        }
    }

    if (ssid.empty()) {
        return "";
    }

    std::vector<std::string> parts;
    size_t start = 0;
    while (start <= ssid.size()) {
        size_t end = ssid.find('-', start);
        if (end == std::string::npos) {
            parts.push_back(ssid.substr(start));
            break;
        }
        parts.push_back(ssid.substr(start, end - start));
        start = end + 1;
    }

    for (size_t i = 0; i < parts.size(); ++i) {
        if (!parts[i].empty() && parts[i][0] == 'M') {
            if (i + 1 < parts.size() && !parts[i + 1].empty()) {
                return parts[i] + "-" + parts[i + 1];
            }
            return parts[i];
        }
    }

    return "";
}


static string trim_copy(const string& value)
{
    size_t start = 0;
    while (start < value.size() && isspace(static_cast<unsigned char>(value[start]))) {
        ++start;
    }

    size_t end = value.size();
    while (end > start && isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }

    return value.substr(start, end - start);
}

static string to_lower_copy(const string& value)
{
    string out;
    out.reserve(value.size());
    for (char c : value) {
        out.push_back(static_cast<char>(tolower(static_cast<unsigned char>(c))));
    }
    return out;
}

static vector<string> split_csv_line(const string& line)
{
    vector<string> result;
    string token;
    stringstream ss(line);
    while (getline(ss, token, ',')) {
        result.push_back(trim_copy(token));
    }
    return result;
}

struct CsvColumns {
    int timestamp = 0;
    int methane = 0;
    int sniffer_methane = 0;
    int distance = 0;
    int latitude = 0;
    int longitude = 0;
    int altitude = 0;
    int targ_latitude = 0;
    int targ_longitude = 0;
    int wind_x = 0;
    int wind_y = 0;
    int wind_z = 0;
};

static int find_column_index(const vector<string>& headers, const vector<string>& candidates)
{
    for (const auto& candidate : candidates) {
        const string target = to_lower_copy(trim_copy(candidate));
        for (size_t i = 0; i < headers.size(); ++i) {
            const string normalized = to_lower_copy(trim_copy(headers[i]));
            if (normalized == target) {
                return static_cast<int>(i);
            }
        }
    }
    return -1;
}

static CsvColumns resolve_columns(const vector<string>& headers)
{
    CsvColumns c;
    c.timestamp = find_column_index(headers,{"Timestamp"});
    c.methane = find_column_index(headers, {"Methane [ppm*m]"});
    c.sniffer_methane = find_column_index(headers, {"Sniffer Methane [ppm]"});
    c.distance = find_column_index(headers,{"Distance [m]"});
    c.latitude = find_column_index(headers, {"RTK Lat [deg]", "Gimbal Lat [deg]", "Targ Lat [deg]", "Center Lat [deg]"});
    c.longitude = find_column_index(headers, {"RTK Lon [deg]", "Gimbal Lon [deg]", "Targ Lon [deg]", "Center Lon [deg]"});
    c.altitude = find_column_index(headers, {"RTK HFSL [m]", "Gimbal HFSL [m]", "Targ HFSL [m]", "Center HFSL [m]"});
    c.targ_latitude = find_column_index(headers, {"Targ Lat [deg]"});
    c.targ_longitude = find_column_index(headers, {"Targ Lon [deg]"});
    c.wind_x = find_column_index(headers, {"Wind U [m/s]"});
    c.wind_y = find_column_index(headers, {"Wind V [m/s]"});
    c.wind_z = find_column_index(headers, {"Wind W [m/s]"});
    return c;
}

static string get_value(const vector<string>& row, int index)
{
    if (index < 0 || static_cast<size_t>(index) >= row.size()) {
        return "";
    }
    return trim_copy(row[static_cast<size_t>(index)]);
}

static string json_number_or_null(const string& raw)
{
    const string v = trim_copy(raw);
    if (v.empty() || v == "#VALUE!" || v == "nan" || v == "NaN") {
        return "null";
    }

    char* end_ptr = nullptr;
    std::strtod(v.c_str(), &end_ptr);
    if (end_ptr == v.c_str() || *end_ptr != '\0') {
        return "null";
    }

    return v;
}

static string build_payload_json(const vector<string>& row, const CsvColumns& cols)
{
    const string timestamp = get_value(row, cols.timestamp);
    const string drone_name = getDroneName();
    const string methane = json_number_or_null(get_value(row, cols.methane));
    const string sniffer = json_number_or_null(get_value(row, cols.sniffer_methane));
    const string distance = json_number_or_null(get_value(row, cols.distance));
    const string latitude = json_number_or_null(get_value(row, cols.latitude));
    const string longitude = json_number_or_null(get_value(row, cols.longitude));
    const string altitude = json_number_or_null(get_value(row, cols.altitude));
    const string targ_latitude = json_number_or_null(get_value(row, cols.latitude));
    const string targ_longitude = json_number_or_null(get_value(row, cols.longitude));
    const string wind_x = json_number_or_null(get_value(row, cols.wind_x));
    const string wind_y = json_number_or_null(get_value(row, cols.wind_y));
    const string wind_z = json_number_or_null(get_value(row, cols.wind_z));

    ostringstream payload;
    payload << "{";
    payload << "\"timestamp\":\"" << timestamp << "\",";
    payload << "\"drone\":\"" << drone_name << "\",";
    payload << "\"methane\":" << methane << ",";
    payload << "\"sniffer_methane\":" << sniffer << ",";
    payload << "\"distance\":\"" << distance << "\",";
    payload << "\"position\":{";
    payload << "\"latitude\":" << latitude << ",";
    payload << "\"longitude\":" << longitude << ",";
    payload << "\"altitude\":" << altitude;
    payload << "},";
    payload << "\"target_position\":{";
    payload << "\"latitude\":" << targ_latitude << ",";
    payload << "\"longitude\":" << targ_longitude;
    payload << "},";
    payload << "\"wind_direction\":{";
    payload << "\"x\":" << wind_x << ",";
    payload << "\"y\":" << wind_y << ",";
    payload << "\"z\":" << wind_z;
    payload << "}";
    payload << "}";

    return payload.str();
}

static string send_at_collect(const string& command, chrono::milliseconds timeout)
{
    while (Serial.available() > 0) {
        Serial.read();
    }

    Serial.println(command.c_str());

    string response;
    const auto idle_timeout = chrono::milliseconds(500);
    auto deadline = chrono::steady_clock::now() + timeout;
    auto last_data = chrono::steady_clock::now();

    while (chrono::steady_clock::now() < deadline) {
        if (Serial.available() > 0) {
            const char received = Serial.read();
            response.push_back(received);
            printf("%c", received);
            last_data = chrono::steady_clock::now();
        } else {
            if (!response.empty() && chrono::steady_clock::now() - last_data >= idle_timeout) {
                break;
            }
            this_thread::sleep_for(chrono::milliseconds(20));
        }
    }

    return response;
}

static bool response_contains(const string& response, const string& needle)
{
    return response.find(needle) != string::npos;
}

static vector<uint8_t> encode_remaining_length(size_t length)
{
    vector<uint8_t> encoded;
    do {
        uint8_t byte = static_cast<uint8_t>(length % 128);
        length /= 128;
        if (length > 0) {
            byte |= 0x80;
        }
        encoded.push_back(byte);
    } while (length > 0);
    return encoded;
}

static string send_binary_collect(const vector<uint8_t>& payload, chrono::milliseconds timeout)
{
    while (Serial.available() > 0) {
        Serial.read();
    }

    for (uint8_t byte : payload) {
        Serial.write(byte);
    }

    string response;
    const auto idle_timeout = chrono::milliseconds(3000);
    auto deadline = chrono::steady_clock::now() + timeout;
    auto last_data = chrono::steady_clock::now();

    while (chrono::steady_clock::now() < deadline) {
        if (Serial.available() > 0) {
            const char received = Serial.read();
            response.push_back(received);
            printf("%c", received);
            last_data = chrono::steady_clock::now();
        } else {
            if (!response.empty() && chrono::steady_clock::now() - last_data >= idle_timeout) {
                break;
            }
            this_thread::sleep_for(chrono::milliseconds(20));
        }
    }

    return response;
}

static string read_serial_collect(chrono::milliseconds timeout, chrono::milliseconds idle_timeout = chrono::milliseconds(600))
{
    string response;
    auto deadline = chrono::steady_clock::now() + timeout;
    auto last_data = chrono::steady_clock::now();

    while (chrono::steady_clock::now() < deadline) {
        if (Serial.available() > 0) {
            const char received = Serial.read();
            response.push_back(received);
            printf("%c", received);
            last_data = chrono::steady_clock::now();
        } else {
            if (!response.empty() && chrono::steady_clock::now() - last_data >= idle_timeout) {
                break;
            }
            this_thread::sleep_for(chrono::milliseconds(20));
        }
    }

    return response;
}

static int parse_mqtt_connack_code(const string& response)
{
    for (size_t i = 0; i + 3 < response.size(); ++i) {
        const uint8_t b0 = static_cast<uint8_t>(response[i]);
        const uint8_t b1 = static_cast<uint8_t>(response[i + 1]);
        if (b0 == 0x20 && b1 == 0x02) {
            return static_cast<int>(static_cast<uint8_t>(response[i + 3]));
        }
    }
    return -1;
}

static const char* mqtt_connack_reason(int code)
{
    switch (code) {
    case 0: return "Connection Accepted";
    case 1: return "Unacceptable Protocol Version";
    case 2: return "Identifier Rejected";
    case 3: return "Server Unavailable";
    case 4: return "Bad Username or Password";
    case 5: return "Not Authorized";
    default: return "Unknown";
    }
}

static bool sim7600_socket_setup()
{
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "AT+CGSOCKCONT=1,\"IP\",\"%s\"", APN);
    if (sim7600.sendATcommand(cmd, "OK", 2000) != 1) {
        fprintf(stderr, "CGSOCKCONT failed\n");
        return false;
    }

    if (sim7600.sendATcommand("AT+CSOCKSETPN=1", "OK", 2000) != 1) {
        fprintf(stderr, "CSOCKSETPN failed\n");
        return false;
    }

    if (sim7600.sendATcommand("AT+CIPMODE=0", "OK", 2000) != 1) {
        fprintf(stderr, "CIPMODE failed\n");
        return false;
    }

    const string netopen_response = send_at_collect("AT+NETOPEN", chrono::milliseconds(10000));
    if (!response_contains(netopen_response, "+NETOPEN: 0") &&
        !response_contains(netopen_response, "Network is already opened")) {
        fprintf(stderr, "NETOPEN failed\n");
        return false;
    }

    if (sim7600.sendATcommand("AT+IPADDR", "+IPADDR:", 3000) != 1) {
        fprintf(stderr, "IPADDR failed\n");
        return false;
    }

    return true;
}

static bool sim7600_open_tcp_socket()
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "AT+CIPOPEN=0,\"TCP\",\"%s\",%d", BROKER_HOST, BROKER_PORT);
    const string response = send_at_collect(cmd, chrono::milliseconds(15000));
    if (!response_contains(response, "+CIPOPEN: 0,0")) {
        fprintf(stderr, "CIPOPEN failed\n");
        return false;
    }
    return true;
}

static vector<uint8_t> build_connect_packet()
{
    const string client_id = MQTT_CLIENT;
    const string username = MQTT_USER;
    const string password = MQTT_PASS;
    vector<uint8_t> packet;
    packet.push_back(0x10);

    uint8_t connect_flags = 0x02;
    if (!username.empty()) {
        connect_flags |= 0x80;
    }
    if (!password.empty()) {
        connect_flags |= 0x40;
    }

    vector<uint8_t> variable_header = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',
        0x04,
        connect_flags,
        0x00, 0x3c,
    };

    vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>((client_id.size() >> 8) & 0xff));
    payload.push_back(static_cast<uint8_t>(client_id.size() & 0xff));
    payload.insert(payload.end(), client_id.begin(), client_id.end());
    if (!username.empty()) {
        payload.push_back(static_cast<uint8_t>((username.size() >> 8) & 0xff));
        payload.push_back(static_cast<uint8_t>(username.size() & 0xff));
        payload.insert(payload.end(), username.begin(), username.end());
    }
    if (!password.empty()) {
        payload.push_back(static_cast<uint8_t>((password.size() >> 8) & 0xff));
        payload.push_back(static_cast<uint8_t>(password.size() & 0xff));
        payload.insert(payload.end(), password.begin(), password.end());
    }

    const size_t remaining = variable_header.size() + payload.size();
    const vector<uint8_t> encoded_length = encode_remaining_length(remaining);
    packet.insert(packet.end(), encoded_length.begin(), encoded_length.end());
    packet.insert(packet.end(), variable_header.begin(), variable_header.end());
    packet.insert(packet.end(), payload.begin(), payload.end());
    return packet;
}

static vector<uint8_t> build_publish_packet(const string& payload)
{
    const string topic = MQTT_TOPIC;
    vector<uint8_t> packet;
    packet.push_back(static_cast<uint8_t>(0x30 | (MQTT_RETAIN ? 0x01 : 0x00)));

    vector<uint8_t> body;
    body.push_back(static_cast<uint8_t>((topic.size() >> 8) & 0xff));
    body.push_back(static_cast<uint8_t>(topic.size() & 0xff));
    body.insert(body.end(), topic.begin(), topic.end());
    body.insert(body.end(), payload.begin(), payload.end());

    const vector<uint8_t> encoded_length = encode_remaining_length(body.size());
    packet.insert(packet.end(), encoded_length.begin(), encoded_length.end());
    packet.insert(packet.end(), body.begin(), body.end());
    return packet;
}

static bool sim7600_send_socket_payload(const vector<uint8_t>& payload, chrono::milliseconds timeout, string* response_out = nullptr)
{
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "AT+CIPSEND=0,%zu", payload.size());
    const string prompt = send_at_collect(cmd, chrono::milliseconds(3000));
    if (!response_contains(prompt, ">")) {
        fprintf(stderr, "CIPSEND prompt failed\n");
        return false;
    }

    const string response = send_binary_collect(payload, timeout);
    if (!response_contains(response, "+CIPSEND: 0")) {
        fprintf(stderr, "CIPSEND failed\n");
        return false;
    }

    if (response_out != nullptr) {
        *response_out = response;
    }
    return true;
}


static void sim7600_cellular_init()
{
    sim7600.PowerOn(POWERKEY);
    sim7600.sendATcommand("ATE0", "OK", 2000);

    sim7600.sendATcommand("AT+CPIN?",  "+CPIN: READY", 3000);
    sim7600.sendATcommand("AT+CSQ",    "OK",           2000);
    sim7600.sendATcommand("AT+CEREG?", "+CEREG: 0,1",  3000);
    sim7600.sendATcommand("AT+CGATT?", "+CGATT: 1",    3000);

    char cmd[128];
    snprintf(cmd, sizeof(cmd), "AT+CGDCONT=1,\"IP\",\"%s\"", APN);
    sim7600.sendATcommand(cmd, "OK", 2000);
    sim7600.sendATcommand("AT+CGACT=1,1",  "OK",        5000);
    sim7600.sendATcommand("AT+CGPADDR=1",  "+CGPADDR:", 2000);

    sim7600.sendATcommand("AT+CDNSGIP=\"google.com\"", "+CDNSGIP:", 8000);
}



static bool sim7600_mqtt_connect()
{
    for (int attempt = 1; attempt <= 3; ++attempt) {
        send_at_collect("AT+CIPCLOSE=0", chrono::milliseconds(3000));
        send_at_collect("AT+NETCLOSE", chrono::milliseconds(5000));

        if (!sim7600_socket_setup()) {
            continue;
        }

        if (!sim7600_open_tcp_socket()) {
            continue;
        }

        string response;
        if (!sim7600_send_socket_payload(build_connect_packet(), chrono::milliseconds(8000), &response)) {
            fprintf(stderr, "MQTT CONNECT send failed on attempt %d\n", attempt);
            continue;
        }

        int connack_code = parse_mqtt_connack_code(response);
        if (connack_code < 0) {
            response += read_serial_collect(chrono::milliseconds(2500));
            connack_code = parse_mqtt_connack_code(response);
        }

        if (connack_code == 0) {
            printf("MQTT connected to %s:%d\n", BROKER_HOST, BROKER_PORT);
            return true;
        }

        if (connack_code >= 0) {
            fprintf(stderr, "MQTT CONNACK failed on attempt %d (code=%d: %s)\n",
                    attempt, connack_code, mqtt_connack_reason(connack_code));
        } else {
            fprintf(stderr, "MQTT CONNACK not received on attempt %d\n", attempt);
        }
        delay(1000);
    }

    return false;
}

static bool check_signal_quality() {
    const string response = send_at_collect("AT+CSQ", chrono::milliseconds(3000));
    if (!response_contains(response, "+CSQ:")) {
        fprintf(stderr, "CSQ command failed\n");
        return false;
    }

    size_t pos = response.find("+CSQ:");
    if (pos == string::npos) {
        return false;
    }

    pos += 5;
    while (pos < response.size() && isspace(response[pos])) {
        pos++;
    }

    int rssi = -1;
    try {
        rssi = stoi(response.substr(pos));
    } catch (...) {
        return false;
    }

    if (rssi == 99) {
        fprintf(stderr, "Signal not detected or unavailable (RSI = 99)\n");
        return false;
    }

    if (rssi < 0 || rssi > 31) {
        fprintf(stderr, "Invalid signal strength\n");
        return false;
    }

    printf("Signal quality: %d\n", rssi);
    return true;
}

static bool udp_fallback(std::string telemetry) {
    boost::asio::io_service io_service;
    udp::socket socket(io_service);
    socket.open(udp::v4());

    boost::asio::ip::address destination_ip = boost::asio::ip::address::from_string("10.42.0.255");
    udp::endpoint remote_endpoint(destination_ip, 5000);

    std::string message = telemetry;

    boost::system::error_code err;
    socket.send_to(boost::asio::buffer(message), remote_endpoint, 0, err);

    if(err) {
        std::cerr << "Error sending message: " << err.message() << std::endl;
        socket.close();
        return false;
    } else {
        std::cout << "Message sent successfully!" << std::endl;
        socket.close();
        return true;
    }
}


static bool publish_payload(const string& payload)
{
    return sim7600_send_socket_payload(build_publish_packet(payload), chrono::milliseconds(1000));
}

static string build_batch_payload(const deque<string>& buffered_payloads)
{
    ostringstream batch;
    batch << "{\"batch\":[";
    for (size_t i = 0; i < buffered_payloads.size(); ++i) {
        if (i > 0) {
            batch << ",";
        }
        batch << buffered_payloads[i];
    }
    batch << "]}";
    return batch.str();
}

static void flush_buffered_payloads(deque<string>& buffered_payloads, bool& using_cellular)
{
    if (buffered_payloads.empty()) {
        return;
    }

    const string batch_payload = build_batch_payload(buffered_payloads);
    fprintf(stdout, "Sending batch with %zu line(s)...\n", buffered_payloads.size());

    if (using_cellular) {
        if (!publish_payload(batch_payload)) {
            fprintf(stderr, "Cellular batch publish failed, checking signal...\n");
            if (!check_signal_quality()) {
                fprintf(stdout, "Signal lost! Falling back to UDP...\n");
                using_cellular = false;
            }
            udp_fallback(batch_payload);
        }
    } else {
        udp_fallback(batch_payload);
    }

    buffered_payloads.clear();
}

static void sim7600_mqtt_disconnect()
{
    const vector<uint8_t> disconnect_packet = {0xe0, 0x00};
    sim7600_send_socket_payload(disconnect_packet, chrono::milliseconds(2000));
    send_at_collect("AT+CIPCLOSE=0", chrono::milliseconds(5000));
    send_at_collect("AT+NETCLOSE", chrono::milliseconds(5000));
}

static bool read_tail_lines(const string& file_path, size_t last_n_lines, string& header_line, deque<string>& lines)
{
    ifstream input(file_path);
    if (!input.is_open()) {
        cerr << "Error opening file: " << file_path << '\n';
        return false;
    }

    if (!getline(input, header_line)) {
        cerr << "CSV file is empty: " << file_path << '\n';
        return false;
    }

    string line;
    while (getline(input, line)) {
        if (trim_copy(line).empty()) {
            continue;
        }
        lines.push_back(line);
        if (lines.size() > last_n_lines) {
            lines.pop_front();
        }
    }

    return true;
}

static void follow_csv_updates_and_publish(
    const string& file_path,
    chrono::milliseconds poll_interval,
    const CsvColumns& cols)
{
    ifstream input(file_path);
    if (!input.is_open()) {
        cerr << "Error opening file: " << file_path << '\n';
        return;
    }

    string header;
    getline(input, header);

    input.seekg(0, ios::end);
    streamoff last_offset = static_cast<streamoff>(input.tellg());
    if (last_offset < 0) {
        last_offset = 0;
    }

    bool using_cellular = true;
    auto last_signal_check = chrono::steady_clock::now();
    const chrono::milliseconds signal_check_interval(5000);

    auto last_send_time = chrono::steady_clock::now();
    const chrono::milliseconds send_interval(1000);
    deque<string> buffered_payloads;

    while (true) {
        this_thread::sleep_for(poll_interval);

        auto now = chrono::steady_clock::now();
        if (now - last_signal_check >= signal_check_interval) {
            last_signal_check = now;

            bool has_signal = check_signal_quality();

            if (has_signal && !using_cellular) {
                fprintf(stdout, "Signal recovered! Reconnecting to cellular MQTT...\n");
                if (sim7600_mqtt_connect()) {
                    using_cellular = true;
                    fprintf(stdout, "Successfully reconnected to cellular MQTT\n");
                } else {
                    fprintf(stderr, "Failed to reconnect to cellular MQTT, staying on UDP fallback\n");
                }
            } else if (!has_signal && using_cellular) {
                fprintf(stdout, "Signal lost! Falling back to UDP...\n");
                using_cellular = false;
            }
        }

        std::error_code ec;
        const uintmax_t current_size = filesystem::file_size(file_path, ec);
        if (ec) {
            continue;
        }

        if (static_cast<uintmax_t>(last_offset) > current_size) {
            input.clear();
            input.close();
            input.open(file_path);
            if (!input.is_open()) {
                continue;
            }

            string skip_header;
            getline(input, skip_header);
            last_offset = static_cast<streamoff>(input.tellg());
            if (last_offset < 0) {
                last_offset = 0;
            }
        }

        if (static_cast<uintmax_t>(last_offset) == current_size) {
            if (now - last_send_time >= send_interval) {
                last_send_time = now;
                flush_buffered_payloads(buffered_payloads, using_cellular);
            }
            continue;
        }

        input.clear();
        input.seekg(last_offset, ios::beg);

        string line;
        while (getline(input, line)) {
            if (trim_copy(line).empty()) {
                continue;
            }

            cout << line << '\n';
            const vector<string> row = split_csv_line(line);
            const string payload = build_payload_json(row, cols);
            cout << "Buffering JSON: " << payload << '\n';
            buffered_payloads.push_back(payload);
        }

        input.clear();
        input.seekg(0, ios::end);
        last_offset = static_cast<streamoff>(input.tellg());
        if (last_offset < 0) {
            last_offset = 0;
        }

        now = chrono::steady_clock::now();
        if (now - last_send_time >= send_interval) {
            last_send_time = now;
            flush_buffered_payloads(buffered_payloads, using_cellular);
        }
    }
}


int main(void)
{
    const string csv_path = "/data/eerl/latest.log";
    const size_t lines_to_tail = 1;
    const chrono::milliseconds poll_interval(1000);

    string header_line;
    deque<string> tail_lines;
    if (!read_tail_lines(csv_path, lines_to_tail, header_line, tail_lines)) {
        return 1;
    }
    const vector<string> headers = split_csv_line(header_line);
    const CsvColumns cols = resolve_columns(headers);

    sim7600_cellular_init();

    bool using_cellular = false;
    if (sim7600_mqtt_connect()) {
        using_cellular = true;
        fprintf(stdout, "Connected to cellular MQTT\n");
    } else {
        fprintf(stderr, "MQTT connection failed, falling back to UDP\n");
        using_cellular = false;
    }

    cout << "Last " << tail_lines.size() << " line(s) from CSV:\n";
    int rc = 0;
    for (const auto& row_line : tail_lines) {
        cout << row_line << '\n';
        const vector<string> row = split_csv_line(row_line);
        const string payload = build_payload_json(row, cols);
        cout << "Publishing JSON: " << payload << '\n';
        
        if (using_cellular) {
            if (!publish_payload(payload)) {
                fprintf(stderr, "Cellular publish failed, checking signal...\n");
                if (!check_signal_quality()) {
                    fprintf(stdout, "Signal lost! Falling back to UDP...\n");
                    using_cellular = false;
                    udp_fallback(payload);
                } else {
                    rc = 3;
                }
            }
        } else {
            udp_fallback(payload);
        }
    }

    follow_csv_updates_and_publish(csv_path, poll_interval, cols);

    if (using_cellular) {
        sim7600_mqtt_disconnect();
    }
    return rc;
}
