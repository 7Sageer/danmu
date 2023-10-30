package sql;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ErrorCollector {
        private ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
    
        public synchronized void addError(String errorMessage) {
            errors.add(errorMessage);
            return;
        }
    
        public synchronized void displayErrors() {
            if (!errors.isEmpty()) {
                System.out.println("Errors encountered:");
                for (String error : errors) {
                    System.out.println("  - " + error);
                }
            } else {
                System.out.println("No errors found. ");
            }
        }
}
