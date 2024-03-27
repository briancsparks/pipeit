package main

// tried: #!/usr/bin env go run, but didn't work

import (
    "bufio"
    "fmt"
    "io"
    "net"
    "os"
    "strconv"
    "strings"
)

func main() {
    cmd, args, positional, remaining := parseCmdArgs(os.Args[1:])

    key := getFlag(args, "key")
    count := mustGetFlagInt(args, "count")
    rate := mustGetFlagFloat(args, "rate")
    verbose := mustGetFlagBool(args, "verbose")

    err := streamToRedis(os.Stdin, key, args)
    if err != nil {
        panic(fmt.Sprintf("Error streaming to Redis: %v", err))
    }

    fmt.Printf("Command: %s\n", cmd)
    fmt.Printf("Lines appended to Redis list with key: %s\n", key)
    fmt.Printf("Positional parameters: %v\n", positional)
    fmt.Printf("Remaining parameters: %v\n", remaining)
    fmt.Printf("Count: %d\n", count)
    fmt.Printf("Rate: %.2f\n", rate)
    fmt.Printf("Verbose: %t\n", verbose)
}

//func streamToRedis(r io.Reader, key, host, port string) error
func streamToRedis(r io.Reader, key string, args map[string]string) error {
    host := getFlag(args, "host")
    port := getFlag(args, "port")     // TODO: Port should be int-type. But Dial takes a string, so we're OK-ish

    conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
    if err != nil {
        return fmt.Errorf("error connecting to Redis: %v", err)
    }
    defer conn.Close()

    scanner := bufio.NewScanner(r)
    for scanner.Scan() {
        line := scanner.Text()
        cmd := fmt.Sprintf("*3\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(line), line)
        _, err := conn.Write([]byte(cmd))
        if err != nil {
            return fmt.Errorf("error sending command to Redis: %v", err)
        }

        // Read the response from Redis
        resp, err := readResponse(conn)
        if err != nil {
            return fmt.Errorf("error reading response from Redis: %v", err)
        }
        // You can process the response if needed
        _ = resp
    }

    if err := scanner.Err(); err != nil {
        return fmt.Errorf("error reading from input: %v", err)
    }

    return nil
}

func readResponse(conn net.Conn) (string, error) {
    reader := bufio.NewReader(conn)
    line, err := reader.ReadString('\n')
    if err != nil {
        return "", err
    }
    return strings.TrimSuffix(line, "\r\n"), nil
}

func parseArgs(args []string) (map[string]string, []string, []string) {
    // ... (parseCmdArgs function implementation remains the same)
    parsed := make(map[string]string)
    var positional []string
    var remaining []string
    dashdash := false

    for _, arg := range args {
        if dashdash {
            remaining = append(remaining, arg)
        } else if arg == "--" {
            dashdash = true
        } else if strings.HasPrefix(arg, "--") {
            parts := strings.Split(arg[2:], "=")
            key := parts[0]
            value := ""
            if len(parts) > 1 {
                value = parts[1]
            }
            parsed[key] = value

            // Add an additional key with underscores
            underscoreKey := strings.ReplaceAll(key, "-", "_")
            parsed[underscoreKey] = value
        } else {
            positional = append(positional, arg)
        }
    }

    return parsed, positional, remaining
}

func parseCmdArgs(args []string) (string, map[string]string, []string, []string) {
    // ... (parseCmdArgs function implementation remains the same)
    parsed := make(map[string]string)
    var cmd string
    var positional []string
    var remaining []string
    dashdash := false

    if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
        cmd = args[0]
        args = args[1:]
    }

    // TODO: Just call parseArgs here.

    for _, arg := range args {
        if dashdash {
            remaining = append(remaining, arg)
        } else if arg == "--" {
            dashdash = true
        } else if strings.HasPrefix(arg, "--") {
            parts := strings.Split(arg[2:], "=")
            key := parts[0]
            value := ""
            if len(parts) > 1 {
                value = parts[1]
            }
            parsed[key] = value

            // Add an additional key with underscores
            underscoreKey := strings.ReplaceAll(key, "-", "_")
            parsed[underscoreKey] = value
        } else {
            positional = append(positional, arg)
        }
    }

    return cmd, parsed, positional, remaining
}

// ... (getFlag, getFlagIntRequired, getFlagFloatRequired, getFlagBoolRequired functions remain the same)


func getFlag(args map[string]string, flag string) string {
    value, ok := args[flag]
    if !ok {
        panic(fmt.Sprintf("Missing required flag: --%s", flag))
    }
    return value
}


func getFlagInt(args map[string]string, flag string, defaultValue int) int {
    value, ok := args[flag]
    if !ok {
        return defaultValue
    }
    i, err := strconv.Atoi(value)
    if err != nil {
        panic(fmt.Sprintf("Invalid value for flag --%s: %s", flag, value))
    }
    return i
}

func getFlagFloat(args map[string]string, flag string, defaultValue float64) float64 {
    value, ok := args[flag]
    if !ok {
        return defaultValue
    }
    f, err := strconv.ParseFloat(value, 64)
    if err != nil {
        panic(fmt.Sprintf("Invalid value for flag --%s: %s", flag, value))
    }
    return f
}

func getFlagBool(args map[string]string, flag string, defaultValue bool) bool {
    value, ok := args[flag]
    if !ok {
        return defaultValue
    }
    b, err := strconv.ParseBool(value)
    if err != nil {
        panic(fmt.Sprintf("Invalid value for flag --%s: %s", flag, value))
    }
    return b
}




func mustGetFlagInt(args map[string]string, flag string) int {
    value, ok := args[flag]
    if !ok {
        panic(fmt.Sprintf("Missing required flag: --%s", flag))
    }
    i, err := strconv.Atoi(value)
    if err != nil {
        panic(fmt.Sprintf("Invalid value for flag --%s: %s", flag, value))
    }
    return i
}

func mustGetFlagFloat(args map[string]string, flag string) float64 {
    value, ok := args[flag]
    if !ok {
        panic(fmt.Sprintf("Missing required flag: --%s", flag))
    }
    f, err := strconv.ParseFloat(value, 64)
    if err != nil {
        panic(fmt.Sprintf("Invalid value for flag --%s: %s", flag, value))
    }
    return f
}

func mustGetFlagBool(args map[string]string, flag string) bool {
    value, ok := args[flag]
    if !ok {
        panic(fmt.Sprintf("Missing required flag: --%s", flag))
    }
    b, err := strconv.ParseBool(value)
    if err != nil {
        panic(fmt.Sprintf("Invalid value for flag --%s: %s", flag, value))
    }
    return b
}
















