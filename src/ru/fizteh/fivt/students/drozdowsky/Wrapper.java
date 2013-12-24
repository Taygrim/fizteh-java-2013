package ru.fizteh.fivt.students.drozdowsky;

import ru.fizteh.fivt.proxy.LoggingProxyFactory;

import javax.xml.stream.XMLStreamException;
import java.io.Writer;
import java.lang.reflect.Proxy;

public class Wrapper implements LoggingProxyFactory {
    @Override
    public Object wrap(Writer writer, Object implementation, Class<?> interfaceClass) {
        if (writer == null || interfaceClass == null || !interfaceClass.isInterface()
                || !interfaceClass.isInstance(implementation)) {
               throw new IllegalArgumentException("bad proxy arguments");
            }
        try {
            return Proxy.newProxyInstance(implementation.getClass().getClassLoader(), new Class[]{interfaceClass},
                    new LoggingXMLProxyInvocationHandler(writer, implementation));
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }
}
