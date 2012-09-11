package com.goeswhere.recorddumper;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class RecordDumper {

    private static final int BLOCK = 500;
    private final ThreadLocal<Session> session;
    private final long start = System.currentTimeMillis();

    public RecordDumper(URI serverUri) throws XccConfigException {
        final ContentSource cs = ContentSourceFactory.newContentSource(serverUri);

        session = new ThreadLocal<Session>() {
            protected Session initialValue() {
                return cs.newSession();
            }
        };
    }


    public void load(String... args) throws RequestException, InterruptedException {
        final ExecutorService files = Executors.newFixedThreadPool(4);
        final ExecutorService writers = new ThreadPoolExecutor(10, 10, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(100) {
            @Override
            public boolean offer(Runnable runnable) {
                try {
                    put(runnable);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }
        });
        for (final String file : args) {
            files.submit(p(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    System.out.println("loading " + file);
                    List<Content> l = new ArrayList<Content>(BLOCK);

                    final Charset utf8 = Charset.forName("UTF-8");
                    final GZIPInputStream zip = new GZIPInputStream(new FileInputStream(file));
                    final BufferedReader r = new BufferedReader(new InputStreamReader(zip, utf8));
                    try {
                        String s;
                        while (null != (s = r.readLine())) {
                            final String uri = uriOf(s);

                            final ContentCreateOptions options = new ContentCreateOptions();
                            options.setFormat(DocumentFormat.XML);
                            options.setCollections(new String[] { uri.replaceAll("_.*","") });

                            l.add(ContentFactory.newContent(uri, s, options));
                            if (l.size() >= BLOCK) {
                                executeContents(writers, l);
                            }
                        }
                        executeContents(writers, l);
                    } finally {
                        r.close();
                    }
                    return null;
                }
            }));
        }

        files.shutdown();
        files.awaitTermination(900, TimeUnit.DAYS);

        writers.shutdown();
        writers.awaitTermination(900, TimeUnit.DAYS);
    }

    private final AtomicLong written = new AtomicLong();

    private void executeContents(ExecutorService writers, List<Content> l) throws InterruptedException {
        if (l.isEmpty()) {
            return;
        }
        final Content[] contents = l.toArray(new Content[BLOCK]);
        final int size = l.size();
        writers.submit(p(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    session.get().insertContent(contents);
                } catch (NullPointerException e) {
                    for (Content c : contents) {
                        System.out.println("NPE: " + c.getUri());
                    }
                    throw e;
                }
                final long soFar = written.addAndGet(size);
                final long target = 24000000l;
                final float tps = soFar * 1000.f / (System.currentTimeMillis() - start);
                System.out.println(Math.round(tps) + " tps, "
                        + Math.round((target - soFar)/tps/60.f) + " mins remain");
                return null;
            }
        }));
        l.clear();
    }

    private <T> Callable<T> p(final Callable<T> callable) {
        return new Callable<T>() {
            @Override
            public T call() throws Exception {
                try {
                    return callable.call();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        };
    }

    private static final AtomicLong GENERATED_ID = new AtomicLong();

    private static final Pattern HREF = Pattern.compile("^.*? href=\"\\[pips-host:(?:/pips)?/?api/v1/([^\"]+)/\\]\"");

    private String uriOf(String s) {
        final Matcher matcher = HREF.matcher(s);
        if (matcher.find() && null != matcher.group(1))
            return matcher.group(1).replaceAll("/","_");
        System.out.println("no uri: " + s.substring(0, Math.min(100, s.length())));
        return String.valueOf(GENERATED_ID.incrementAndGet());
    }

    public static void main(String[] args)
            throws URISyntaxException, XccConfigException, RequestException, InterruptedException,
            MalformedURLException {

        RecordDumper loader = new RecordDumper(new URI("xcc://loader:loader@localhost:9000"));
        if (0 == args.length) {
            System.out.println("usage: file1 [file2..]");
            return;
        }

        loader.load(args);
    }
}
