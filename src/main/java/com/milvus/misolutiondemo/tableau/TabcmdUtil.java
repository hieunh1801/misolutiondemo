package com.milvus.misolutiondemo.tableau;

import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TabcmdUtil {
    public static void runCommand(String[] command) throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        log.info(">>> " + String.join(" ", command));
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        boolean finished = process.waitFor(30, TimeUnit.MINUTES); // Timeout sau 30 giây

        if (!finished) {
            process.destroyForcibly(); // Kill process
            throw new RuntimeException("Command timed out after 30 minutes");
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            StringBuilder linesBuilder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                linesBuilder.append(line).append("\n");
            }

            String lines = linesBuilder.toString();
            if (lines.contains("Error") || lines.contains("error") || lines.contains("400011: Bad Request") || lines.contains("PublishingException")) {
                throw new RuntimeException("Tabcmd command failed: \n\n" + lines);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        log.info("Hello World");
        String[] commandToRun = {"tabcmd"};

        runCommand(commandToRun);
//
//        System.out.println("\nKết quả đầu ra của lệnh:");
//        System.out.println("-------------------------");
//        System.out.println(commandOutput);
//        System.out.println("-------------------------");
    }
}
