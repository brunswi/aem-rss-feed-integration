package com.mysite.core.services.impl;

import com.adobe.cq.dam.cfm.ContentElement;
import com.adobe.cq.dam.cfm.ContentFragment;
import com.adobe.cq.dam.cfm.ContentFragmentException;
import com.adobe.cq.dam.cfm.FragmentData;
import com.adobe.cq.dam.cfm.FragmentTemplate;
import com.day.cq.polling.importer.HCImporter;
import com.day.cq.polling.importer.Importer;
import com.google.common.collect.ImmutableMap;
import com.rometools.modules.mediarss.MediaEntryModule;
import com.rometools.modules.mediarss.MediaModule;
import com.rometools.modules.mediarss.types.MediaContent;
import com.rometools.modules.mediarss.types.UrlReference;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.entity.ContentType;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.propertytypes.ServiceDescription;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
    service = {Importer.class},
    immediate = true,
    property = {
        Importer.SCHEME_PROPERTY + "=rss"
    },
    configurationPolicy = ConfigurationPolicy.REQUIRE
)
@ServiceDescription("Feed Importer")
@Designate(ocd = FeedImporter.Config.class)
public class FeedImporter extends HCImporter implements Importer {

    private static final Logger LOG = LoggerFactory.getLogger(FeedImporter.class);

    private static final String CFM_PATH = "/conf/rss-feed-integration/settings/dam/cfm/models/feed-item";

    @Reference
    private ResourceResolverFactory resolverFactory;

    private static final String SERVICE_USER = "feedimporter-writer";

    @Override
    public void importData(final String scheme, final InputStream data, final String characterEncoding, final Resource target) throws IOException {
        String encoding = characterEncoding != null ? characterEncoding : "utf-8";
        try (ResourceResolver resourceResolver = this.resolverFactory.getServiceResourceResolver(
            ImmutableMap.of(ResourceResolverFactory.SUBSERVICE, SERVICE_USER))) {
            SyndFeedInput input = new SyndFeedInput();
            SyndFeed feed = input.build(new InputStreamReader(data, encoding));
            for (SyndEntry entry : feed.getEntries()) {
                createContentFragment(entry, resourceResolver, target);
            }
            resourceResolver.commit();
        } catch (LoginException|FeedException e) {
            LOG.error(e.getMessage(), e);
        }

    }
    private void createContentFragment(SyndEntry entry, ResourceResolver resourceResolver, Resource target) {
        String title = entry.getTitle();
        String link = entry.getLink();
        // publish date
        Calendar publishedAt = Calendar.getInstance();
        publishedAt.setTime(entry.getPublishedDate());
        // use publishedAt and link hash as resource name / id
        String id = publishedAt.getTimeInMillis() + "-" + DigestUtils.md2Hex(link);
        // check if id already exists. if not, create content fragment for feed item
        if (resourceResolver.getResource(target, id) == null) {
            // optional description
            String description = null;
            if (entry.getDescription() != null) {
                description = entry.getDescription().getValue();
            }
            // optional image
            String imageUrl = null;
            MediaEntryModule mediaModule = (MediaEntryModule) entry.getModule(MediaModule.URI);
            if (mediaModule != null && mediaModule.getMediaContents().length > 0) {
                for (MediaContent mediaContent : mediaModule.getMediaContents()) {
                    if (mediaContent.getReference() instanceof UrlReference) {
                        imageUrl = ((UrlReference) mediaContent.getReference()).getUrl().toString();
                    }
                }
            }
            // create content fragment
            try {
                Resource parentResource = resourceResolver.getResource(target.getPath());
                FragmentTemplate fragmentTemplate = resourceResolver.getResource(CFM_PATH)
                    .adaptTo(FragmentTemplate.class);
                ContentFragment contentFragment = fragmentTemplate.createFragment(parentResource, id, title);
                resourceResolver.commit();
                contentFragment.getElement("title")
                    .setContent(title, ContentType.TEXT_PLAIN.getMimeType());
                if (description != null) {
                    contentFragment.getElement("description")
                        .setContent(description, ContentType.TEXT_PLAIN.getMimeType());
                }
                contentFragment.getElement("link")
                    .setContent(link, ContentType.TEXT_PLAIN.getMimeType());
                if (imageUrl != null) {
                    contentFragment.getElement("imageUrl")
                        .setContent(imageUrl, ContentType.TEXT_PLAIN.getMimeType());
                }
                ContentElement publishedAtElement = contentFragment.getElement("publishedAt");
                FragmentData publishedAtData = publishedAtElement.getValue();
                publishedAtData.setValue(publishedAt);
                publishedAtElement.setValue(publishedAtData);
                resourceResolver.commit();
            } catch (ContentFragmentException | PersistenceException e) {
                LOG.warn("item: {}, error: {}", entry.getTitle(), e.getMessage());
            }
        }
    }

    @ObjectClassDefinition(name = "Feed Importer")
    public @interface Config {

        @AttributeDefinition(name = "Enabled",
            description = "Enabled (true/false)")
        boolean enabled() default false;
    }

}
