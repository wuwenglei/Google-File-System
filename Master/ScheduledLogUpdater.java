import java.util.logging.*;
import java.nio.file.*;
import java.io.IOException;
import java.util.*;
import java.rmi.UnexpectedException;

public class ScheduledLogUpdater implements Runnable {
    private static final Logger NAMESPACE_LOGGER_OVERWRITE = Logger.getLogger("namespaceOverwrite");
    private static final Logger MAPPING_LOGGER_OVERWRITE = Logger.getLogger("mappingOverwrite");
    private static final Logger CHUNKSERVER_LOGGER_OVERWRITE = Logger.getLogger("chunkserverOverwrite");
    private static final String namespaceLogFileNameOverwrite = "./namespace_tmp.log";
    private static final String mappingLogFileNameOverwrite = "./mapping_tmp.log";
    private static final String chunkserverLogFileNameOverwrite = "./chunkserver_tmp.log";
    private int choice;

    public ScheduledLogUpdater(int choice) {
        Master.initializeLogger(NAMESPACE_LOGGER_OVERWRITE, namespaceLogFileNameOverwrite, false);
        Master.initializeLogger(MAPPING_LOGGER_OVERWRITE, mappingLogFileNameOverwrite, false);
        Master.initializeLogger(CHUNKSERVER_LOGGER_OVERWRITE, chunkserverLogFileNameOverwrite, false);
        this.choice = choice;
    }

    @Override
    public void run() {
        if (choice == 0) {
            updateNamespace();
        } else if (choice == 1) {
            updateMapping();
        } else if (choice == 2) {
            updateChunkserver();
        }
    }

    private void updateNamespace() {
        for (Name name: Master.namespace.values()) {
            Master.writeLogNamespace(name, NAMESPACE_LOGGER_OVERWRITE);
        }
        replaceFile(namespaceLogFileNameOverwrite, Master.namespaceLogFileName);
    }

    private void updateMapping() {
        for (Map.Entry<Name, List<Chunk>> entry: Master.mapping.entrySet()) {
            Master.writeLogMapping(entry, MAPPING_LOGGER_OVERWRITE);
        }
        replaceFile(mappingLogFileNameOverwrite, Master.mappingLogFileName);
    }

    private void updateChunkserver() {
        Master.registeredChunkserver.forEach((chunkserverID) -> {
            try {
                Master.writeLogChunkServer(chunkserverID, CHUNKSERVER_LOGGER_OVERWRITE);
            } catch (UnexpectedException e) {
                e.printStackTrace();
            }
        });
        replaceFile(chunkserverLogFileNameOverwrite, Master.chunkserverLogFileName);
    }

    private void replaceFile(String s, String d) {
        Path source = Paths.get(s);
        try{
            Files.move(source, source.resolveSibling(d), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}
