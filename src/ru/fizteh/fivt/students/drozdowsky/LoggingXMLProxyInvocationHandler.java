package ru.fizteh.fivt.students.drozdowsky;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.IdentityHashMap;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class LoggingXMLProxyInvocationHandler implements InvocationHandler{

    Object implementation;
    XMLStreamWriter xmlWriter;
    Writer writer;
    IdentityHashMap<Object, Boolean> writtenObjects;

    public LoggingXMLProxyInvocationHandler(Writer writer, Object implementation) throws XMLStreamException {
        this.implementation = implementation;
        this.writer = writer;
        this.writtenObjects = new IdentityHashMap<>();
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = null;
        Throwable error = null;
        try {
            result = method.invoke(implementation, args);
        } catch (InvocationTargetException e) {
            error = e.getTargetException();
        } catch (Throwable e) {
            error = e;
        }

        if (method.getDeclaringClass() != Object.class) {
            try {
                StringWriter xmlResult  = new StringWriter();
                this.xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(xmlResult);
                xmlWriter.writeStartElement("invoke");
                xmlWriter.writeAttribute("timestamp", ((Long) System.currentTimeMillis()).toString());
                xmlWriter.writeAttribute("class", implementation.getClass().getName());
                xmlWriter.writeAttribute("name", method.getName());

                if (args != null && args.length > 0) {
                    xmlWriter.writeStartElement("arguments");
                    for (Object arg: args) {
                        xmlWriter.writeStartElement("argument");
                        writeObject(arg);
                        xmlWriter.writeEndElement();
                    }
                    xmlWriter.writeEndElement();
                } else {
                    xmlWriter.writeEmptyElement("arguments");
                }

                if (method.getReturnType() != void.class && error == null) {
                    xmlWriter.writeStartElement("return");
                    writeObject(result);
                    xmlWriter.writeEndElement();
                }

                if (error != null) {
                    xmlWriter.writeStartElement("thrown");
                    xmlWriter.writeCharacters(error.toString());
                    xmlWriter.writeEndElement();
                }

                xmlWriter.writeEndElement();
                writer.write(xmlWriter.toString() + System.lineSeparator());
            } catch (Exception e) {
                //Not allowed
            }
        }
        if (error != null) {
            throw error;
        }
        return result;
    }

    private void writeObject(Object object) throws XMLStreamException {
        if (object == null) {
            xmlWriter.writeEmptyElement("null");
        } else if (object instanceof Iterable) {
            writtenObjects.clear();
            writeIterable((Iterable) object);
        } else {
            xmlWriter.writeCharacters(object.toString());
        }
    }

    private void writeIterable(Iterable object) throws XMLStreamException {
        if (writtenObjects.get(object) != null) {
            xmlWriter.writeCharacters("cyclic");
        } else {
            writtenObjects.put(object, true);
            xmlWriter.writeStartElement("list");

            for (Object value : object) {
                xmlWriter.writeStartElement("value");
                writeObject(value);
                xmlWriter.writeEndElement();
            }

            xmlWriter.writeEndElement();
            writtenObjects.remove(object);
        }
    }
}
