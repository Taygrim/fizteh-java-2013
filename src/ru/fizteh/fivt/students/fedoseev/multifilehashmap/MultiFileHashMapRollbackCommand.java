package ru.fizteh.fivt.students.fedoseev.multifilehashmap;

import ru.fizteh.fivt.students.fedoseev.common.AbstractCommand;

import java.io.IOException;

public class MultiFileHashMapRollbackCommand extends AbstractCommand<MultiFileHashMapState> {
    public MultiFileHashMapRollbackCommand() {
        super("rollback", 0);
    }

    @Override
    public void execute(String[] input, MultiFileHashMapState state) throws IOException {
        MultiFileHashMapTable curTable = state.getCurTable();

        if (curTable == null) {
            System.out.println("no table");
            throw new IOException("ERROR: not existing table");
        } else {
            System.out.println(curTable.rollback());
        }
    }
}
